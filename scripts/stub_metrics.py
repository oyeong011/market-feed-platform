#!/usr/bin/env python3
"""지표 한 줄을 그대로 내주는 최소 익스포터. **알람 전달 경로 시험용이다.**

알람이 사람에게 닿는지 확인하려면 알람이 울어야 하고, 알람이 울려면 지표가 임계를
넘어야 한다. 실제 스택에서 공백이나 누수를 일부러 만드는 건 느리고 불안정하다.
그래서 값만 내주는 과녁을 둔다 — 나머지(수집 설정 · 규칙 · 라우팅 · 수신기)는
전부 실제 파일 그대로 쓴다. 흉내 내는 것은 지표 값 하나뿐이다.

    python scripts/stub_metrics.py --port 9103 --set 'mdfeed_data_gaps_open 1'
    curl -X POST 'localhost:9103/set' -d 'mdfeed_data_gaps_open 0'   # 값 바꾸기
"""
from __future__ import annotations

import argparse
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

_LOCK = threading.Lock()
_BODY: list[str] = []


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):     # noqa: A002
        pass

    def _send(self, code: int, body: bytes, ctype="text/plain; version=0.0.4") -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:              # noqa: N802
        path = urlparse(self.path).path
        if path == "/metrics":
            with _LOCK:
                body = ("\n".join(_BODY) + "\n").encode()
            self._send(200, body)
        elif path == "/healthz":
            self._send(200, b'{"healthy":true}', "application/json")
        else:
            self._send(404, b"not found")

    def do_POST(self) -> None:             # noqa: N802
        if urlparse(self.path).path != "/set":
            self._send(404, b"not found")
            return
        length = int(self.headers.get("Content-Length") or 0)
        lines = [ln.strip() for ln in self.rfile.read(length).decode().splitlines()
                 if ln.strip()]
        with _LOCK:
            _BODY[:] = lines
        print(f"[과녁] 값 교체: {lines}", flush=True)
        self._send(200, b"ok")


def main() -> int:
    ap = argparse.ArgumentParser("stub_metrics")
    ap.add_argument("--host", default="0.0.0.0")      # noqa: S104
    ap.add_argument("--port", type=int, default=9103)
    ap.add_argument("--set", action="append", default=[],
                    help="처음에 낼 지표 줄. 여러 번 줄 수 있다")
    args = ap.parse_args()
    with _LOCK:
        _BODY[:] = args.set or ["mdfeed_stub_up 1"]
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"과녁 익스포터 :{args.port}/metrics → {_BODY}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
