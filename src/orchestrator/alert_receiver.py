"""Alert receiver: an Alertmanager webhook target that logs each alert as a JSON line.

Shortcut: stands in for a real pager (PagerDuty/Slack); logs land in Loki.
"""

import json
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from orchestrator import config
from orchestrator.logging import setup

log = setup("alert-receiver", socket.gethostname())


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self._respond(200 if self.path == "/healthz" else 404)

    def do_POST(self):
        if self.path != "/alerts":
            return self._respond(404)
        try:
            payload = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        except ValueError:
            log.error("alert.malformed_payload")
            return self._respond(400)
        for alert in payload.get("alerts", []):
            labels, annotations = alert.get("labels", {}), alert.get("annotations", {})
            fields = {
                "alertname": labels.get("alertname"),
                "severity": labels.get("severity"),
                "status": alert.get("status"),
                "summary": annotations.get("summary"),
                "startsAt": alert.get("startsAt"),
                "labels": labels,
            }
            if alert.get("status") == "firing":
                log.warning("alert.firing", extra=fields)
            else:
                log.info("alert.resolved", extra=fields)
        self._respond(200)

    def _respond(self, status: int) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):  # silence default access logs
        pass


def main() -> None:
    log.info("alert_receiver.started", extra={"port": config.ALERT_RECEIVER_LISTEN_PORT})
    ThreadingHTTPServer(("", config.ALERT_RECEIVER_LISTEN_PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
