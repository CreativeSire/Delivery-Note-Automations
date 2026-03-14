from __future__ import annotations

import argparse
import json
import mimetypes
import shutil
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from uuid import uuid4


XML_RESPONSE = """<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER>
    <STATUS>1</STATUS>
  </HEADER>
  <BODY>
    <DATA>
      <CREATED>1</CREATED>
      <ALTERED>0</ALTERED>
      <LASTVCHID>{message}</LASTVCHID>
      <TALLYMESSAGE>{message}</TALLYMESSAGE>
    </DATA>
  </BODY>
</ENVELOPE>
"""

ALLOWED_REGISTER_SUFFIXES = {".xlsx", ".xls", ".csv", ".tsv", ".txt"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local bridge agent for the hosted DALA Tally Bridge.")
    parser.add_argument("--host", default="0.0.0.0", help="Host interface to bind to. Default: 0.0.0.0")
    parser.add_argument("--port", type=int, default=9000, help="Port to bind to. Default: 9000")
    parser.add_argument(
        "--base-dir",
        default=r"C:\TallyBridge",
        help=r"Base folder on the Tally server. Default: C:\TallyBridge",
    )
    parser.add_argument("--inbox", default="inbox", help="Inbound payload folder name under the base dir.")
    parser.add_argument("--outbox", default="outbox", help="Returned register folder name under the base dir.")
    parser.add_argument("--archive", default="archive", help="Archive folder name under the base dir.")
    return parser.parse_args()


class BridgeState:
    def __init__(self, *, base_dir: Path, inbox_dir: Path, outbox_dir: Path, archive_dir: Path) -> None:
        self.base_dir = base_dir
        self.inbox_dir = inbox_dir
        self.outbox_dir = outbox_dir
        self.archive_dir = archive_dir


def ensure_dirs(base_dir: Path, inbox_name: str, outbox_name: str, archive_name: str) -> BridgeState:
    inbox_dir = base_dir / inbox_name
    outbox_dir = base_dir / outbox_name
    archive_dir = base_dir / archive_name
    for folder in (base_dir, inbox_dir, outbox_dir, archive_dir):
        folder.mkdir(parents=True, exist_ok=True)
    return BridgeState(base_dir=base_dir, inbox_dir=inbox_dir, outbox_dir=outbox_dir, archive_dir=archive_dir)


def build_handler(state: BridgeState):
    class TallyBridgeAgentHandler(BaseHTTPRequestHandler):
        server_version = "DALA-Tally-Bridge-Agent/1.0"

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "status": "ok",
                        "service": "dala-tally-bridge-agent",
                        "base_dir": str(state.base_dir),
                        "inbox_dir": str(state.inbox_dir),
                        "outbox_dir": str(state.outbox_dir),
                        "archive_dir": str(state.archive_dir),
                    },
                )
                return

            if parsed.path == "/register/latest":
                query = parse_qs(parsed.query)
                claim = query.get("claim", ["0"])[0] == "1"
                register_path = select_latest_register(state.outbox_dir)
                if register_path is None:
                    self._send_text(HTTPStatus.NOT_FOUND, "No returned register file is available yet.")
                    return

                payload = register_path.read_bytes()
                content_type = mimetypes.guess_type(register_path.name)[0] or "application/octet-stream"
                archive_path = archive_register(register_path, state.archive_dir)
                if claim:
                    register_path.unlink(missing_ok=True)

                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("X-DALA-Register-Filename", register_path.name)
                self.send_header("X-DALA-Archive-Path", str(archive_path))
                self.end_headers()
                self.wfile.write(payload)
                return

            self._send_text(HTTPStatus.NOT_FOUND, "Unknown path.")

        def do_POST(self) -> None:
            content_length = int(self.headers.get("Content-Length", "0") or "0")
            payload = self.rfile.read(content_length) if content_length > 0 else b""
            content_type = (self.headers.get("Content-Type") or "").lower()

            if is_probe_payload(payload, content_type):
                response = XML_RESPONSE.format(message="Local bridge reachable")
                self._send_xml(HTTPStatus.OK, response)
                return

            filename = self.headers.get("X-DALA-Filename") or f"sales-order-{uuid4().hex}.xlsx"
            saved_path = save_inbound_payload(state.inbox_dir, filename, payload)
            response = XML_RESPONSE.format(message=f"saved:{saved_path.name}")
            self._send_xml(HTTPStatus.OK, response)

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}] {self.address_string()} - {format % args}")

        def _send_xml(self, status: HTTPStatus, body: str) -> None:
            encoded = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/xml; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def _send_json(self, status: HTTPStatus, body: dict[str, object]) -> None:
            encoded = json.dumps(body, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def _send_text(self, status: HTTPStatus, body: str) -> None:
            encoded = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    return TallyBridgeAgentHandler


def is_probe_payload(payload: bytes, content_type: str) -> bool:
    if "xml" in content_type:
        return True
    sample = payload.decode("utf-8", errors="ignore").lower()
    return "<envelope" in sample and "<body" in sample


def save_inbound_payload(inbox_dir: Path, original_name: str, payload: bytes) -> Path:
    safe_name = Path(original_name).name or f"payload-{uuid4().hex}.bin"
    target = inbox_dir / f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid4().hex[:8]}-{safe_name}"
    target.write_bytes(payload)
    return target


def select_latest_register(outbox_dir: Path) -> Path | None:
    candidates = [
        path
        for path in outbox_dir.iterdir()
        if path.is_file() and path.suffix.lower() in ALLOWED_REGISTER_SUFFIXES
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def archive_register(register_path: Path, archive_dir: Path) -> Path:
    archive_path = archive_dir / f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{register_path.name}"
    shutil.copy2(register_path, archive_path)
    return archive_path


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir)
    state = ensure_dirs(base_dir, args.inbox, args.outbox, args.archive)
    handler = build_handler(state)
    server = ThreadingHTTPServer((args.host, args.port), handler)

    print("DALA Tally Bridge Agent")
    print(f"Listening on http://{args.host}:{args.port}")
    print(f"Inbox:   {state.inbox_dir}")
    print(f"Outbox:  {state.outbox_dir}")
    print(f"Archive: {state.archive_dir}")
    print("")
    print("Place returned Tally register files into the outbox folder.")
    print("The hosted DALA app will push payloads into the inbox folder and pull back the latest register over HTTP.")
    print("")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping bridge agent.")


if __name__ == "__main__":
    main()
