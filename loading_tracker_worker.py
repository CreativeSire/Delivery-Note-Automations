from __future__ import annotations

import sys

from app import app as flask_app
from loading_tracker_services import run_loading_tracker_import_job


def main() -> int:
    if len(sys.argv) < 2:
        return 1
    job_id = sys.argv[1]
    with flask_app.app_context():
        run_loading_tracker_import_job(job_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
