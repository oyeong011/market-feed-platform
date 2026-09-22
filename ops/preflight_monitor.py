#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType
from typing import ClassVar


def _load_preflight() -> ModuleType:
    module_path = Path(__file__).resolve().with_name("preflight.py")
    spec = importlib.util.spec_from_file_location("mdfeed_ops_preflight", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load preflight module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


PREFLIGHT = _load_preflight()


class Handler(BaseHTTPRequestHandler):
    config_path: ClassVar[str] = ""

    def do_GET(self) -> None:
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        result = PREFLIGHT.run(self.config_path)
        body = PREFLIGHT._prometheus(result).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> int:
    parser = argparse.ArgumentParser("mdfeed-preflight-monitor")
    _ = parser.add_argument("--config", required=True)
    _ = parser.add_argument("--host", default="127.0.0.1")
    _ = parser.add_argument("--port", type=int, default=9120)
    args = parser.parse_args()
    Handler.config_path = str(args.config)
    server = ThreadingHTTPServer((str(args.host), int(args.port)), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
