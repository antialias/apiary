"""A stand-in for the Anthropic API, for the compose smoke test only (smoke/run.sh).

The brain validates and polls an API key with GET /v1/models; the proxy forwards a class's
request to POST /v1/messages. Both get a minimal well-formed answer. Every request's headers
are kept, and GET /_seen returns them, so the smoke can check what credential arrived.
Stdlib only; it listens on 0.0.0.0:8080 inside the compose network and calls nothing."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_seen = []
_lock = threading.Lock()


class Stub(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, code, body):
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _record(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n) if n else b""
        with _lock:
            _seen.append({"method": self.command, "path": self.path,
                          "headers": {k.lower(): v for k, v in self.headers.items()},
                          "body": body.decode("utf-8", "replace")})
        return body

    def do_GET(self):
        if self.path == "/_seen":
            with _lock:
                return self._send(200, list(_seen))
        self._record()
        if self.path.split("?")[0] == "/v1/models":
            return self._send(200, {"data": [{"type": "model", "id": "claude-sonnet-5-5",
                                              "display_name": "Stub Sonnet"}],
                                    "has_more": False, "first_id": "claude-sonnet-5-5",
                                    "last_id": "claude-sonnet-5-5"})
        self._send(404, {"type": "error", "error": {"type": "not_found_error", "message": self.path}})

    def do_POST(self):
        body = self._record()
        if self.path.split("?")[0] == "/v1/messages":
            try:
                model = json.loads(body or b"{}").get("model", "")
            except ValueError:
                model = ""
            return self._send(200, {
                "id": "msg_stub", "type": "message", "role": "assistant", "model": model,
                "content": [{"type": "text", "text": "stub reply"}],
                "stop_reason": "end_turn", "stop_sequence": None,
                "usage": {"input_tokens": 3, "output_tokens": 2}})
        self._send(404, {"type": "error", "error": {"type": "not_found_error", "message": self.path}})

    def log_message(self, fmt, *args):
        print("stub: " + fmt % args, flush=True)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Stub).serve_forever()
