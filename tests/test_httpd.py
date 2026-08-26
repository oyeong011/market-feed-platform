"""직접 구현한 HTTP/1.1 서버 — 프로토콜 정확성과 보안."""
import asyncio
import json
import socket

import pytest

from mdfeed.httpd import HTTPServer, Request, Response, health_routes, _parse_request
from mdfeed.metrics import Registry


def raw_request(port, data: bytes, read_all: bool = False) -> bytes:
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.sendall(data)
    out = b""
    s.settimeout(2.0)
    try:
        while True:
            c = s.recv(65536)
            if not c:
                break
            out += c
            if not read_all and b"\r\n\r\n" in out:
                # 본문까지 받았는지 Content-Length 로 판단
                head, _, body = out.partition(b"\r\n\r\n")
                for line in head.split(b"\r\n"):
                    if line.lower().startswith(b"content-length:"):
                        if len(body) >= int(line.split(b":")[1]):
                            return out
                break
    except socket.timeout:
        pass
    finally:
        s.close()
    return out


async def _serve(routes, port):
    reg = Registry("test")
    srv = HTTPServer("127.0.0.1", port, "test", reg)
    for m, p, h in routes:
        srv.route(m, p, h)
    health_routes(srv, lambda: {"healthy": True, "note": "한글 본문"})
    await srv.start()
    return srv


def run_server_test(fn, port=18711, routes=()):
    async def main():
        srv = await _serve(routes, port)
        try:
            return await asyncio.to_thread(fn, port)
        finally:
            await srv.close()
    return asyncio.run(main())


def test_healthz_and_metrics():
    def check(port):
        r = raw_request(port, b"GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        assert b"200 OK" in r
        body = r.split(b"\r\n\r\n", 1)[1]
        assert json.loads(body)["healthy"] is True
        m = raw_request(port, b"GET /metrics HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        assert b"mdfeed_uptime_seconds" in m
    run_server_test(check)


def test_content_length_counts_bytes_not_characters():
    """한글 응답에서 len(str) 을 쓰면 본문이 잘려 브라우저가 멈춘다."""
    def check(port):
        r = raw_request(port, b"GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        head, _, body = r.partition(b"\r\n\r\n")
        clen = next(int(l.split(b":")[1]) for l in head.split(b"\r\n")
                    if l.lower().startswith(b"content-length:"))
        assert clen == len(body)
        assert "한글 본문" in body.decode("utf-8")
    run_server_test(check)


def test_keep_alive_serves_two_requests_on_one_connection():
    def check(port):
        s = socket.create_connection(("127.0.0.1", port), timeout=5)
        s.sendall(b"GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n")
        first = s.recv(65536)
        assert b"keep-alive" in first.lower()
        s.sendall(b"GET /readyz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        second = s.recv(65536)
        s.close()
        assert b"200 OK" in second
    run_server_test(check, port=18712)


def test_head_has_no_body():
    def check(port):
        r = raw_request(port, b"HEAD /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
                        read_all=True)
        head, _, body = r.partition(b"\r\n\r\n")
        assert b"200 OK" in head and body == b""
    run_server_test(check, port=18713)


def test_unknown_path_404():
    def check(port):
        r = raw_request(port, b"GET /nope HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        assert b"404" in r
    run_server_test(check, port=18714)


def test_method_not_allowed_405():
    def check(port):
        r = raw_request(port, b"POST /healthz HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n"
                              b"Connection: close\r\n\r\n")
        assert b"405" in r
    run_server_test(check, port=18715)


def test_handler_exception_returns_500_not_crash():
    def boom(_req):
        raise RuntimeError("의도된 예외")

    def check(port):
        r = raw_request(port, b"GET /boom HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        assert b"500" in r
        # 서버가 살아있는지 확인
        assert b"200 OK" in raw_request(
            port, b"GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
    run_server_test(check, port=18716, routes=[("GET", "/boom", boom)])


def test_malformed_request_line_400():
    def check(port):
        r = raw_request(port, b"NOT-HTTP\r\n\r\n")
        assert b"400" in r
    run_server_test(check, port=18717)


class TestRequestParsing:
    def test_query_parsing(self):
        r = _parse_request(b"GET /a?x=1&y=hi HTTP/1.1\r\nHost: h\r\n\r\n")
        assert r.path == "/a" and r.query == {"x": "1", "y": "hi"}

    def test_percent_decoding(self):
        r = _parse_request(b"GET /a/%ED%95%9C HTTP/1.1\r\nHost: h\r\n\r\n")
        assert r.path == "/a/한"

    def test_q_int_clamps_out_of_range(self):
        r = _parse_request(b"GET /a?limit=999999 HTTP/1.1\r\nHost: h\r\n\r\n")
        assert r.q_int("limit", 10, 1, 500) == 500
        r2 = _parse_request(b"GET /a?limit=abc HTTP/1.1\r\nHost: h\r\n\r\n")
        assert r2.q_int("limit", 10, 1, 500) == 10


def test_static_path_traversal_blocked(tmp_path):
    secret = tmp_path.parent / "secret.txt"
    secret.write_text("절대 노출되면 안 되는 값")
    (tmp_path / "index.html").write_text("<h1>ok</h1>")

    async def main():
        srv = HTTPServer("127.0.0.1", 18718, "test", Registry("t"))
        srv.static("/", str(tmp_path))
        await srv.start()
        try:
            def check(port):
                good = raw_request(port, b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                assert b"<h1>ok</h1>" in good
                bad = raw_request(
                    port, b"GET /../secret.txt HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                assert b"404" in bad and "절대".encode() not in bad
            await asyncio.to_thread(check, 18718)
        finally:
            await srv.close()
    asyncio.run(main())


def test_static_root_wins_over_health_index_route(tmp_path):
    """루트에 정적 파일을 마운트하면 health_routes 의 `GET /` JSON 라우트를 이겨야 한다.

    실제로 `make demo` 안내대로 localhost:9102 를 열었을 때 대시보드 대신
    JSON 이 나왔다. 정적 마운트가 더 명시적인 의사표시이므로 그쪽이 이긴다.
    /healthz · /metrics 는 그대로 살아있어야 한다.
    """
    (tmp_path / "index.html").write_text("<!DOCTYPE html><h1>대시보드</h1>")

    async def main():
        srv = HTTPServer("127.0.0.1", 18719, "test", Registry("t"))
        health_routes(srv, lambda: {"healthy": True})
        srv.static("/", str(tmp_path))          # health_routes 뒤에 마운트
        await srv.start()
        try:
            def check(port):
                root = raw_request(port, b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                assert b"text/html" in root, "루트가 HTML 이 아님"
                assert "대시보드".encode() in root
                # 관리 엔드포인트는 그대로여야 한다
                assert b"200 OK" in raw_request(
                    port, b"GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                assert b"mdfeed_uptime" in raw_request(
                    port, b"GET /metrics HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            await asyncio.to_thread(check, 18719)
        finally:
            await srv.close()
    asyncio.run(main())
