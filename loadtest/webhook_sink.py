import logging
import os
import secrets
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


logger = logging.getLogger("webhook_sink")

FAILURE_RATE = float(os.environ.get("WEBHOOK_FAILURE_RATE", "0.1"))
PORT = int(os.environ.get("WEBHOOK_SINK_PORT", "8080"))


def _should_fail(path: str) -> bool:
    return path == "/fail" or secrets.randbelow(10_000) < FAILURE_RATE * 10_000


class _WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        status = HTTPStatus.INTERNAL_SERVER_ERROR if _should_fail(self.path) else HTTPStatus.OK
        logger.info(f"POST {self.path} -> {status.value}")
        self.send_response(status)
        self.end_headers()

    def log_message(self, *_: object) -> None:
        pass


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    logger.info(f"Listening on :{PORT}, failure rate {FAILURE_RATE:.0%}")
    ThreadingHTTPServer(("0.0.0.0", PORT), _WebhookHandler).serve_forever()  # noqa: S104


if __name__ == "__main__":
    main()
