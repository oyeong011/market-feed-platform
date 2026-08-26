"""최소 HTTP/1.1 서버 (표준 라이브러리만) — 모든 서비스의 공통 관리/배포 평면.

담당하는 것
-----------
* 모든 프로세스의 `/healthz`, `/readyz`, `/metrics` (운영 점검의 단일 규약)
* REST 시세 조회 API
* 정적 대시보드 서빙
* WebSocket 업그레이드 (같은 포트에서 HTTP와 WS를 함께 받는다)

직접 짠 이유는 wsproto.py 와 같다. 그리고 실제로 다뤄야 하는 것들이 있다:
* keep-alive 와 `Connection: close` 처리 — 이걸 틀리면 브라우저가 응답을 기다리며 멈춘다
* Content-Length 정확도 — 한글 응답에서 len(str) 과 len(bytes) 를 혼동하면 바로 깨진다
* HEAD 는 본문을 보내지 않는다
* 요청 헤더 크기 상한 — 없으면 느린 클라이언트 하나로 메모리가 샌다(Slowloris)
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import mimetypes
import os
import time
from typing import Awaitable, Callable
from urllib.parse import parse_qs, unquote, urlparse

log = logging.getLogger("mdfeed.httpd")

MAX_HEADER_BYTES = 16 * 1024
READ_TIMEOUT_S = 30.0

STATUS_TEXT = {
    200: "OK", 204: "No Content", 400: "Bad Request", 404: "Not Found",
    405: "Method Not Allowed", 426: "Upgrade Required",
    500: "Internal Server Error", 503: "Service Unavailable",
}


class Request:
    __slots__ = ("method", "path", "query", "headers", "body", "raw_target")

    def __init__(self, method, target, headers, body=b""):
        self.method = method
        self.raw_target = target
        u = urlparse(target)
        self.path = unquote(u.path)
        self.query = {k: v[0] for k, v in parse_qs(u.query).items()}
        self.headers = headers
        self.body = body

    def q_int(self, key: str, default: int, lo: int = 1, hi: int = 10000) -> int:
        try:
            return max(lo, min(hi, int(self.query.get(key, default))))
        except (TypeError, ValueError):
            return default


class Response:
    __slots__ = ("status", "body", "content_type", "headers")

    def __init__(self, status: int = 200, body: bytes | str = b"",
                 content_type: str = "text/plain; charset=utf-8",
                 headers: dict | None = None):
        self.status = status
        self.body = body.encode("utf-8") if isinstance(body, str) else body
        self.content_type = content_type
        self.headers = headers or {}

    @classmethod
    def json(cls, obj, status: int = 200) -> "Response":
        return cls(status, json.dumps(obj, ensure_ascii=False, default=str),
                   "application/json; charset=utf-8")

    @classmethod
    def text(cls, s: str, status: int = 200) -> "Response":
        return cls(status, s)

    def render(self, keep_alive: bool, head_only: bool = False) -> bytes:
        lines = [f"HTTP/1.1 {self.status} {STATUS_TEXT.get(self.status, 'OK')}"]
        lines.append(f"Content-Type: {self.content_type}")
        lines.append(f"Content-Length: {len(self.body)}")     # 반드시 바이트 길이
        lines.append(f"Connection: {'keep-alive' if keep_alive else 'close'}")
        # 대시보드가 다른 오리진에서 붙을 수 있게 (읽기 전용 API라 안전)
        lines.append("Access-Control-Allow-Origin: *")
        for k, v in self.headers.items():
            lines.append(f"{k}: {v}")
        head = ("\r\n".join(lines) + "\r\n\r\n").encode()
        return head if head_only else head + self.body


Handler = Callable[[Request], "Response | Awaitable[Response]"]


class HTTPServer:
    """라우팅 + keep-alive + WebSocket 업그레이드 훅을 갖춘 asyncio HTTP 서버."""

    def __init__(self, host: str, port: int, name: str = "mdfeed", registry=None):
        self.host, self.port, self.name = host, port, name
        self.registry = registry
        self._routes: dict[tuple[str, str], Handler] = {}
        self._prefix: list[tuple[str, str, Handler]] = []
        self._ws_handler = None
        self._ws_path = "/ws"
        self._static: tuple[str, str] | None = None
        self._server = None
        self._conns: set = set()      # 종료 시 끊어야 할 연결 (아래 close() 참고)
        self.requests = 0

    # ── 라우팅 ────────────────────────────────────────────────────────────
    def route(self, method: str, path: str, handler: Handler) -> None:
        self._routes[(method.upper(), path)] = handler

    def prefix(self, method: str, path: str, handler: Handler) -> None:
        self._prefix.append((method.upper(), path, handler))

    def websocket(self, path: str, handler) -> None:
        """handler(reader, writer, request) 로 소켓 소유권을 넘긴다."""
        self._ws_path, self._ws_handler = path, handler

    def static(self, url_prefix: str, directory: str) -> None:
        """정적 파일 마운트.

        루트("/")에 마운트하면 health_routes 가 등록한 `GET /` JSON 라우트를
        걷어낸다. 안 그러면 대시보드를 열어야 할 자리에서 JSON 이 나온다 —
        실제로 `make demo` 안내대로 localhost:9102 를 열었을 때 그랬다.
        정적 마운트가 명시적 의사표시이므로 그쪽이 이긴다.
        """
        prefix = url_prefix.rstrip("/") or "/"
        self._static = (prefix, os.path.abspath(directory))
        if prefix == "/":
            self._routes.pop(("GET", "/"), None)

    # ── 수명주기 ──────────────────────────────────────────────────────────
    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        log.info("[%s] HTTP listening on %s:%d", self.name, self.host, self.port)

    async def close(self) -> None:
        # keep-alive 연결과 WebSocket 은 핸들러가 read() 에서 대기 중이다.
        # Python 3.12+ 의 wait_closed() 는 핸들러 종료를 기다리므로, 먼저 끊지 않으면
        # 종료가 멈춘다. 실제로 이 프로젝트에서 SIGTERM 이 먹지 않는 원인이었다.
        for task, writer in list(self._conns):
            with contextlib.suppress(Exception):
                writer.close()
            if task is not None and not task.done():
                task.cancel()
        pending = [t for t, _ in self._conns if t is not None and not t.done()]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._conns.clear()
        if self._server:
            self._server.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._server.wait_closed(), timeout=5.0)

    # ── 요청 처리 ─────────────────────────────────────────────────────────
    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        conn = (asyncio.current_task(), writer)
        self._conns.add(conn)
        try:
            while True:
                try:
                    raw = await asyncio.wait_for(
                        reader.readuntil(b"\r\n\r\n"), timeout=READ_TIMEOUT_S)
                except (asyncio.IncompleteReadError, asyncio.TimeoutError,
                        ConnectionResetError):
                    return
                except asyncio.LimitOverrunError:
                    writer.write(Response(400, "헤더가 너무 큽니다").render(False))
                    return
                if len(raw) > MAX_HEADER_BYTES:
                    writer.write(Response(400, "헤더가 너무 큽니다").render(False))
                    return

                req = _parse_request(raw)
                if req is None:
                    writer.write(Response(400, "잘못된 요청").render(False))
                    return
                self.requests += 1
                if self.registry:
                    self.registry.counter("http_requests_total")

                # 본문 (POST 등)
                clen = int(req.headers.get("content-length", "0") or 0)
                if clen > 0:
                    req.body = await reader.readexactly(min(clen, 4 * 1024 * 1024))

                # WebSocket 업그레이드?
                if (self._ws_handler
                        and req.path == self._ws_path
                        and req.headers.get("upgrade", "").lower() == "websocket"):
                    # 소켓 소유권을 WS 핸들러로 넘긴다. 다만 종료 시 끊을 수 있도록
                    # 연결 등록은 유지한 채로 넘긴다.
                    await self._ws_handler(reader, writer, req)
                    return

                resp = await self._dispatch(req)
                keep = (req.headers.get("connection", "keep-alive").lower() != "close"
                        and resp.status < 500)
                writer.write(resp.render(keep, head_only=(req.method == "HEAD")))
                await writer.drain()
                if not keep:
                    return
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            pass
        except Exception as e:                   # noqa: BLE001
            log.warning("[%s] http handler error: %s", self.name, e)
        finally:
            self._conns.discard(conn)
            with contextlib.suppress(Exception):
                writer.close()

    async def _dispatch(self, req: Request) -> Response:
        method = "GET" if req.method == "HEAD" else req.method
        h = self._routes.get((method, req.path))
        if h is None:
            for m, p, fn in self._prefix:
                if m == method and req.path.startswith(p):
                    h = fn
                    break
        if h is None and self._static:
            r = self._serve_static(req)
            if r is not None:
                return r
        if h is None:
            if any(p == req.path for (_, p) in self._routes):
                return Response(405, "허용되지 않은 메서드")
            return Response.json({"error": "not found", "path": req.path}, 404)
        try:
            r = h(req)
            return await r if asyncio.iscoroutine(r) else r
        except Exception as e:                   # noqa: BLE001
            log.exception("[%s] handler 예외 %s", self.name, req.path)
            if self.registry:
                self.registry.counter("http_errors_total")
            return Response.json({"error": type(e).__name__, "detail": str(e)}, 500)

    def _serve_static(self, req: Request) -> Response | None:
        url_prefix, root = self._static
        if not req.path.startswith(url_prefix):
            return None
        rel = req.path[len(url_prefix):].lstrip("/") or "index.html"
        # 경로 탈출 방지: 정규화 후에도 root 안에 있는지 확인한다
        target = os.path.abspath(os.path.join(root, rel))
        if not target.startswith(root + os.sep) and target != root:
            return Response(404, "not found")
        if os.path.isdir(target):
            target = os.path.join(target, "index.html")
        if not os.path.isfile(target):
            return None
        ctype = mimetypes.guess_type(target)[0] or "application/octet-stream"
        if ctype.startswith(("text/", "application/json", "application/javascript")):
            ctype += "; charset=utf-8"
        with open(target, "rb") as fh:
            return Response(200, fh.read(), ctype,
                            {"Cache-Control": "no-cache"})


def _parse_request(raw: bytes) -> Request | None:
    try:
        text = raw.decode("latin-1")
        head, _ = text.split("\r\n\r\n", 1)
        lines = head.split("\r\n")
        parts = lines[0].split(" ")
        if len(parts) != 3 or not parts[2].startswith("HTTP/"):
            return None
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        return Request(parts[0].upper(), parts[1], headers)
    except Exception:                            # noqa: BLE001
        return None


def health_routes(server: HTTPServer, health_fn, ready_fn=None) -> None:
    """모든 서비스가 동일한 점검 규약을 갖도록 강제하는 헬퍼.

    /healthz : 프로세스가 살아있는가 (liveness)  → 실패하면 재시작해야 한다
    /readyz  : 트래픽을 받을 준비가 됐는가 (readiness) → 실패하면 잠시 빼야 한다
    둘을 나누는 이유: 기동 직후 DB 연결 전인 상태를 '죽었다'고 판정해 재시작하면
    영원히 못 뜬다. 실제 운영에서 자주 나오는 재시작 루프의 원인이다.
    """
    def _health(_req):
        d = health_fn()
        return Response.json(d, 200 if d.get("healthy", True) else 503)

    def _ready(_req):
        d = ready_fn() if ready_fn else {"ready": True}
        return Response.json(d, 200 if d.get("ready", True) else 503)

    def _metrics(_req):
        if not server.registry:
            return Response(503, "no registry")
        return Response(200, server.registry.prometheus(),
                        "text/plain; version=0.0.4; charset=utf-8")

    server.route("GET", "/healthz", _health)
    server.route("GET", "/readyz", _ready)
    server.route("GET", "/metrics", _metrics)
    server.route("GET", "/", lambda r: Response.json({
        "service": server.name, "endpoints": ["/healthz", "/readyz", "/metrics"],
        "ts": time.time()}))
