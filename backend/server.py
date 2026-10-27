"""
Medicore backend server (standard library only -- no extra dependencies).

    python3 backend/server.py                 # http://127.0.0.1:8000
    python3 backend/server.py --port 8080 --db /tmp/medicore.db

Serves two things from one origin so the whole app runs with a single command:
  * the static frontend (../frontend)           ->  /, /pages/*.html, /css/*, /js/*
  * the checkpoint-recovery JSON API            ->  /api/*

The API drives the real AdaptiveMultilevelScheduler (amq_scheduler.py) through
RecoveryManager (checkpoint_recovery.py). Checkpoints live in SQLite, so they
survive restarting this process; the live scheduler object does not -- which is
exactly what the "restart" demo is about.
"""

import argparse
import json
import mimetypes
import os
import sys
import traceback
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from checkpoint_recovery import CheckpointStore, RecoveryManager, RecoveryError

FRONTEND_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "frontend"))
MAX_BODY_BYTES = 64 * 1024


# --------------------------------------------------------------------------
# API routing (pure function => easy to unit-test without sockets)
# --------------------------------------------------------------------------

def _int(body: dict, key: str, default=None):
    value = body.get(key, default)
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise RecoveryError(f"'{key}' must be a number", 400)
    try:
        return int(value)
    except (TypeError, ValueError):
        raise RecoveryError(f"'{key}' must be a number", 400)


def handle_api(manager: RecoveryManager, method: str, path: str, body: dict, query: dict = None):
    """Return (http_status, json_payload). Never raises for expected failures."""
    query = query or {}
    try:
        route = path.rstrip("/")
        if method == "GET":
            if route == "/api/health":
                return 200, {"ok": True, "service": "medicore-backend"}
            if route == "/api/recovery/status":
                return 200, manager.status()
            if route == "/api/recovery/checkpoints":
                sid = manager.session_id or (manager.store.latest_session() or {}).get("session_id")
                return 200, {"checkpoints": manager.store.list_checkpoints(sid, 100) if sid else []}
            if route == "/api/recovery/events":
                sid = manager.session_id or (manager.store.latest_session() or {}).get("session_id")
                return 200, {"events": manager.store.events(sid, 200) if sid else []}
        elif method == "POST":
            if route == "/api/recovery/session":
                workload = body.get("workload", "hospital")
                if not isinstance(workload, str):
                    raise RecoveryError("'workload' must be a string", 400)
                return 200, manager.start_session(workload, _int(body, "seed", 1), _int(body, "auto_interval", 0))
            if route == "/api/recovery/step":
                return 200, manager.step(_int(body, "ticks", 1), bool(body.get("until_done", False)))
            if route == "/api/recovery/checkpoint":
                label = str(body.get("label", ""))[:80]
                result = manager.checkpoint(label=label)
                result["status"] = manager.status()
                return 200, result
            if route == "/api/recovery/interrupted-write":
                result = manager.simulate_interrupted_write()
                result["status"] = manager.status()
                return 200, result
            if route == "/api/recovery/fail-process":
                pid = body.get("pid")
                if not isinstance(pid, str) or not pid:
                    raise RecoveryError("'pid' is required", 400)
                return 200, manager.fail_process(pid, _int(body, "recovery_failures", 0))
            if route == "/api/recovery/crash-scheduler":
                return 200, manager.crash_scheduler()
            if route == "/api/recovery/restore":
                return 200, manager.restore(_int(body, "checkpoint_id", None))
            if route == "/api/recovery/recover":
                pid = body.get("pid")
                if pid is not None and not isinstance(pid, str):
                    raise RecoveryError("'pid' must be a string", 400)
                return 200, manager.recover_failed(pid)
            if route == "/api/recovery/reset":
                return 200, manager.reset_storage()
        else:
            return 405, {"error": f"method {method} not allowed"}
        return 404, {"error": f"no such endpoint: {method} {path}"}
    except RecoveryError as exc:
        return exc.status, {"error": exc.message}
    except Exception:                                # pragma: no cover - last-resort guard
        traceback.print_exc()
        return 500, {"error": "internal error while handling the request"}


# --------------------------------------------------------------------------
# HTTP plumbing
# --------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    manager: RecoveryManager = None          # injected by make_server()
    server_version = "MedicoreServer/1.0"

    def log_message(self, fmt, *args):       # quieter than the default
        if os.environ.get("MEDICORE_VERBOSE"):
            super().log_message(fmt, *args)

    # -- helpers --
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _send_json(self, status: int, payload: dict):
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self._cors()
        self.end_headers()
        self.wfile.write(data)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length > MAX_BODY_BYTES:
            raise RecoveryError("request body too large", 413)
        if length == 0:
            return {}
        try:
            parsed = json.loads(self.rfile.read(length).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise RecoveryError("request body is not valid JSON", 400)
        if not isinstance(parsed, dict):
            raise RecoveryError("request body must be a JSON object", 400)
        return parsed

    # -- verbs --
    def do_OPTIONS(self):
        self.send_response(HTTPStatus.NO_CONTENT)
        self._cors()
        self.end_headers()

    def do_GET(self):
        url = urlparse(self.path)
        if url.path.startswith("/api/"):
            status, payload = handle_api(self.manager, "GET", url.path, {}, parse_qs(url.query))
            return self._send_json(status, payload)
        self._serve_static(url.path)

    def do_POST(self):
        url = urlparse(self.path)
        if not url.path.startswith("/api/"):
            return self._send_json(404, {"error": "not found"})
        try:
            body = self._read_body()
        except RecoveryError as exc:
            return self._send_json(exc.status, {"error": exc.message})
        status, payload = handle_api(self.manager, "POST", url.path, body)
        self._send_json(status, payload)

    # -- static files --
    def _serve_static(self, url_path: str):
        rel = url_path.lstrip("/") or "index.html"
        target = os.path.abspath(os.path.join(FRONTEND_DIR, rel))
        if os.path.commonpath([target, FRONTEND_DIR]) != FRONTEND_DIR:      # path-traversal guard
            return self._send_json(403, {"error": "forbidden"})
        if os.path.isdir(target):
            target = os.path.join(target, "index.html")
        if not os.path.isfile(target):
            return self._send_json(404, {"error": "not found"})
        ctype = mimetypes.guess_type(target)[0] or "application/octet-stream"
        with open(target, "rb") as fh:
            data = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype + ("; charset=utf-8" if ctype.startswith("text/") or ctype.endswith("javascript") else ""))
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data)


def make_server(host: str = "127.0.0.1", port: int = 8000, db_path: str = None):
    manager = RecoveryManager(CheckpointStore(db_path))
    handler = type("BoundHandler", (Handler,), {"manager": manager})
    return ThreadingHTTPServer((host, port), handler), manager


def main():
    ap = argparse.ArgumentParser(description="Medicore backend + frontend server")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--db", default=None, help="SQLite file (default: backend/data/medicore.db or $MEDICORE_DB)")
    args = ap.parse_args()
    try:
        httpd, manager = make_server(args.host, args.port, args.db)
    except OSError as exc:
        sys.exit(f"Could not start server on {args.host}:{args.port}: {exc}")
    except RecoveryError as exc:
        sys.exit(f"Could not open checkpoint storage: {exc.message}")
    print(f"Medicore running at http://{args.host}:{args.port}/   (checkpoint DB: {manager.store.db_path})")
    print("Press Ctrl+C to stop.")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
