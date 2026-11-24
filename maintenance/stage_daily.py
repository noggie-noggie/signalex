"""Stage Food/VMS independently. Never publish, run AI, or advance a cursor on failure."""
import argparse
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys

from maintenance.collect import SOURCES, WINDOW_SOURCES


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path, required=True)
    p.add_argument("--state", type=Path, required=True)
    p.add_argument("--first-day", type=date.fromisoformat, required=True)
    p.add_argument("--source", choices=SOURCES, action="append")
    a = p.parse_args()
    os.umask(0o077)
    today = datetime.now(timezone.utc).date()
    end = today - timedelta(days=1)  # never certify the current incomplete UTC day
    if not a.db.is_file() or a.first_day > end:
        p.error("Existing DB and first completed UTC day required")
    a.state.mkdir(parents=True, exist_ok=True)
    target = a.state / "database.path"
    if not target.exists():
        with target.open("x") as f:
            f.write(str(a.db.resolve()))
    if target.read_text() != str(a.db.resolve()):
        p.error("State belongs to a different database; use a separate state directory")
    run = a.state / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run.mkdir()
    failures = []
    for source in a.source or SOURCES:
        checkpoint = a.state / (source + ".cursor")
        cmd = [sys.executable, "-m", "maintenance.collect", source, str(run), "--db", str(a.db.resolve())]
        bounded = source in WINDOW_SOURCES or source == "food_fsanz_recalls"
        if bounded:
            since = a.first_day
            if checkpoint.exists():
                since = max(since, date.fromisoformat(checkpoint.read_text().strip()))  # overlap one day
            until = min(since + timedelta(days=6), end)
            cmd.extend(["--since", since.isoformat(), "--until", until.isoformat()])
        else:
            cmd.extend(["--days", "7"])
        try:
            proc = subprocess.run(cmd, timeout=3600, check=False)
            if proc.returncode or not bounded:
                failures.append(source)
                continue
            manifest = json.loads((run / (source + ".json")).read_text())
            if manifest["coverage"] != "query_window_verified" or manifest["until"] != until.isoformat():
                raise ValueError("Missing coverage evidence")
            tmp = run / (source + ".cursor")
            tmp.write_text(until.isoformat())
            tmp.replace(checkpoint)
            if until < end:
                failures.append(source + ": backlog")
        except (OSError, subprocess.TimeoutExpired, KeyError, ValueError) as exc:
            failures.append(source + ": " + type(exc).__name__)
    print("Staging only:", run, "incomplete:", failures, flush=True)
    return 2 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
