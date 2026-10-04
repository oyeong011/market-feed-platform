#!/usr/bin/env python3
"""Alertmanager 가 보낸 알람을 받아 보관하는 개발용 수신기.

**왜 필요한가.** 이 저장소에는 알람 규칙이 48개 있었는데 Alertmanager 가 없었다.
규칙은 Prometheus 안에서 `firing` 상태가 되고 거기서 끝났다 — UI 를 열어 보는 사람이
없으면 아무 일도 일어나지 않는다. 규칙 파일 머리에는 "울리면 사람이 할 일이 있어야
한다" 고 적혀 있었는데, 그 사람에게 닿는 경로가 없었다.

운영에서는 이 자리에 이메일·Slack·PagerDuty 가 온다. 개발 스택과 CI 에는 그걸 붙일
수 없으므로, **도착했는지 물어볼 수 있는 가장 작은 수신기**를 둔다. 그래야
"알람이 사람에게 간다" 를 설정 문자열이 아니라 실행으로 확인할 수 있다.

    POST /alerts     Alertmanager webhook 이 보내는 곳
    GET  /alerts     받은 것 전부 (JSON)
    GET  /alerts?alertname=MarketDataGapOpen   그것만
    GET  /healthz
    GET  /metrics    받은 개수 (Prometheus 형식)

    python ops/alert_sink.py --port 9140
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

_LOCK = threading.Lock()
_RECEIVED: list[dict] = []
_MAX = 500          # 끝없이 쌓지 않는다. 개발용 수신기가 메모리를 먹으면 안 된다
_COUNTS: dict[str, int] = {}


def _record(payload: dict) -> int:
    """Alertmanager 의 webhook 묶음을 알람 단위로 쪼개 보관한다."""
    added = 0
    with _LOCK:
        for alert in payload.get("alerts", []):
            labels = alert.get("labels", {})
            item = {
                "received_at": time.time(),
                "status": alert.get("status") or payload.get("status"),
                "alertname": labels.get("alertname", "(없음)"),
                "severity": labels.get("severity", "(없음)"),
                "service": labels.get("service", ""),
                "receiver": payload.get("receiver", ""),
                "summary": (alert.get("annotations") or {}).get("summary", ""),
                "action": (alert.get("annotations") or {}).get("action", ""),
                "labels": labels,
            }
            _RECEIVED.append(item)
            key = f'{item["alertname"]}|{item["severity"]}|{item["status"]}'
            _COUNTS[key] = _COUNTS.get(key, 0) + 1
            added += 1
        if len(_RECEIVED) > _MAX:
            del _RECEIVED[:-_MAX]
    return added


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):     # noqa: A002 - 기본 로그가 너무 시끄럽다
        pass

    def _send(self, code: int, body: bytes, ctype: str = "application/json") -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:             # noqa: N802
        if urlparse(self.path).path != "/alerts":
            self._send(404, b'{"error":"not found"}')
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            payload = json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, json.JSONDecodeError) as e:
            self._send(400, json.dumps({"error": str(e)}).encode())
            return
        n = _record(payload)
        # 표준출력에 한 줄 남긴다. CI 로그에서 바로 보이는 게 중요하다.
        for alert in payload.get("alerts", [])[:20]:
            labels = alert.get("labels", {})
            print(f"[알람 수신] {labels.get('severity','?'):<8} "
                  f"{labels.get('alertname','?'):<34} {alert.get('status','?'):<8} "
                  f"{(alert.get('annotations') or {}).get('summary','')}", flush=True)
        self._send(200, json.dumps({"received": n}).encode())

    def do_GET(self) -> None:              # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            self._send(200, b'{"healthy":true}')
            return
        if parsed.path == "/alerts":
            q = parse_qs(parsed.query)
            want = q.get("alertname", [None])[0]
            status = q.get("status", [None])[0]
            with _LOCK:
                items = [a for a in _RECEIVED
                         if (want is None or a["alertname"] == want)
                         and (status is None or a["status"] == status)]
            self._send(200, json.dumps(
                {"count": len(items), "items": items}, ensure_ascii=False).encode())
            return
        if parsed.path == "/metrics":
            with _LOCK:
                lines = ["# HELP mdfeed_alerts_received_total 수신기가 받은 알람 수",
                         "# TYPE mdfeed_alerts_received_total counter"]
                for key, n in sorted(_COUNTS.items()):
                    name, sev, st = key.split("|")
                    lines.append('mdfeed_alerts_received_total'
                                 f'{{alertname="{name}",severity="{sev}",status="{st}"}} {n}')
            body = ("\n".join(lines) + "\n").encode()
            self._send(200, body, "text/plain; version=0.0.4")
            return
        self._send(404, b'{"error":"not found"}')


def main() -> int:
    ap = argparse.ArgumentParser("alert_sink")
    ap.add_argument("--host", default="0.0.0.0")      # noqa: S104 - 컨테이너 안에서 쓴다
    ap.add_argument("--port", type=int, default=9140)
    args = ap.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"알람 수신기 http://{args.host}:{args.port}/alerts  "
          f"(POST=수신 · GET=확인 · /metrics=개수)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
