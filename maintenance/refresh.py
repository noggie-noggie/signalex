"""Linux/WSL operator CLI: audit, full, daily, status. No AI or dispatch integrations."""
import argparse
from collections import defaultdict
from contextlib import closing, contextmanager
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
from urllib.parse import urlsplit
from uuid import uuid4

from maintenance.collect import FOOD, SOURCES, WINDOW_SOURCES
from maintenance.reconcile import backup, merge, prepared
from maintenance.snapshot import snapshot
from services.food_taxonomy import enrich_food_signal
from services.vms_evidence import classify


BOUNDED = WINDOW_SOURCES | {"food_fsanz_recalls"}
SNAPSHOTS = {"open_food_facts", "food_fsanz_updates", "artg"}
TABLE = "maintenance_refresh_receipts"
ENABLE = "SIGNALEX_REFRESH_PUBLISH"


def utc():
    return datetime.now(timezone.utc).isoformat()


def save(path, data):
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf8") as f:
        json.dump(data, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


def read(path):
    return json.loads(path.read_text(encoding="utf8"))


def connect(db, mode="ro"):
    return sqlite3.connect(db.resolve(strict=True).as_uri()+"?mode="+mode, uri=True, timeout=15)


@contextmanager
def exclusive(db):
    # Lock the canonical database, not the selected staging directory. Linux flock is
    # independent of SQLite's POSIX byte-range locks; normal API reads still work.
    import fcntl
    with db.open("rb") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("Maintenance already running for this database")
        yield


def receipts(conn):
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)).fetchone():
        return []
    return [json.loads(r[0]) for r in conn.execute(f"SELECT result FROM {TABLE} ORDER BY rowid")]


def verify_baseline(db, before):
    with closing(connect(db)) as c:
        c.execute("ATTACH DATABASE ? AS baseline",(before.resolve(strict=True).as_uri()+"?mode=ro",))
        if c.execute("""SELECT id,source_id,domain,created_at FROM baseline.signals EXCEPT
                        SELECT id,source_id,domain,created_at FROM main.signals LIMIT 1""").fetchone():
            raise ValueError("Lost or changed original identity")
        for left,right in (("baseline","main"),("main","baseline")):
            if c.execute(f"SELECT * FROM {left}.signals WHERE coalesce(domain,'') NOT IN ('food','vms') "
                         f"EXCEPT SELECT * FROM {right}.signals LIMIT 1").fetchone():
                raise ValueError("Out-of-scope signals differ from baseline")


def next_window(source, start, end, daily, prior):
    if not daily:
        return start, end
    if source not in BOUNDED:
        return max(start, end-timedelta(days=6)), end
    cursor = start
    for r in sorted(prior, key=lambda x:x["since"]):
        if r["source"] == source and r["coverage"] == "query_window_verified":
            lo, hi = date.fromisoformat(r["since"]), date.fromisoformat(r["until"])
            if lo <= cursor <= hi:
                cursor = hi + timedelta(days=1)
    # Revisit the latest completed day when caught up; don't leap over failed intervals.
    cursor = min(cursor, end)
    return cursor, min(cursor+timedelta(days=6), end)


def gaps(source, lo, hi, prior):
    cursor = lo
    missing = []
    for r in sorted(prior,key=lambda x:x["since"]):
        if r["source"] != source or r["coverage"] != "query_window_verified":
            continue
        start, end = date.fromisoformat(r["since"]),date.fromisoformat(r["until"])
        if start > hi or end < cursor:
            continue
        if start > cursor:
            missing.append([str(cursor),str(start-timedelta(days=1))])
        cursor = end+timedelta(days=1)
    if cursor <= hi:
        missing.append([str(cursor),str(hi)])
    return missing


def collect(source, folder, db, since, until):
    cmd = [sys.executable, "-m", "maintenance.collect", source, str(folder), "--db", str(db)]
    if source in WINDOW_SOURCES | FOOD:
        cmd += ["--since", str(since), "--until", str(until)]
    else:
        cmd += ["--days", str((date.today()-since).days)]
    return subprocess.run(cmd, timeout=3600, check=False).returncode


def source_gate(source, m, code, lo, hi):
    if m.get("source") != source or not isinstance(m.get("rows"), list):
        raise ValueError("Wrong source or invalid manifest")
    http = m.get("http")
    if (m.get("status") != "parsed" or m.get("error") or m.get("incomplete_queries") or
            not http or any(not isinstance(s,int) or not 200 <= s < 400 for _,s in http)
            or not any(200 <= s < 300 for _,s in http)):
        codes = sorted({str(s) for _,s in http or []})
        raise ValueError(f"Incomplete fetch/parser: {m.get('error') or 'see manifest'}; HTTP {codes}")
    if source in BOUNDED | FOOD and (m["since"],m["until"]) != (str(lo),str(hi)):
        raise ValueError("Collector returned a different interval")
    if source in BOUNDED and code == 0 and m.get("coverage") == "query_window_verified":
        return "query_window_verified"
    if source in SNAPSHOTS and code == 2 and m.get("coverage") == "not_verified" and m["rows"]:
        return "observations_only"
    raise ValueError("No verified publication scope for this source")


def validated(source, raw, lo, hi):
    if not isinstance(raw, dict) or not all(isinstance(raw.get(k), str) and raw[k].strip()
            for k in ("source_id","authority","url","title","scraped_at")):
        raise ValueError("Missing source identity")
    url = urlsplit(raw["url"])
    if url.scheme != "https" or not url.hostname or url.username or url.password or url.port not in (None,443):
        raise ValueError("Invalid provenance URL")
    stamp = datetime.fromisoformat(raw["scraped_at"].replace("Z","+00:00"))
    if source == "open_food_facts":
        code = re.match(r"/product/(\d+)(?:/|$)",url.path)
        identity = code[1] if code else ""
    elif source in FOOD:
        identity = raw["url"]+"::"+raw["title"][:80]
    else:
        identity = raw["url"]
    if raw["source_id"] != hashlib.sha256(f"{source}::{identity}".encode()).hexdigest()[:16]:
        raise ValueError("Source ID does not match provenance")
    if source not in FOOD:
        if raw.get("domain") not in (None,"","vms"):
            raise ValueError("Domain mismatch")
        result = classify(source, raw)
        if not result:
            raise ValueError("VMS evidence requires review")
        return result
    if not lo <= stamp.date() <= hi or not (raw.get("summary") or "").strip():
        raise ValueError("Missing source text or out-of-window Food date")
    if source == "open_food_facts":
        ok = (raw["authority"] == source and (url.hostname == "openfoodfacts.org" or url.hostname.endswith(".openfoodfacts.org"))
              and re.fullmatch(r"/product/\d+(?:/[^/]*)?/?",url.path))
    else:
        prefix = ("/food-recalls/recall-alert/",) if source.endswith("_recalls") else ("/media/","/news/","/food-standards-code/")
        ok = raw["authority"] == "fsanz" and url.hostname in ("foodstandards.gov.au","www.foodstandards.gov.au") and url.path.startswith(prefix)
    if not ok:
        raise ValueError("Food source/URL mismatch")
    record = prepared(source, raw)
    if not record:
        raise ValueError("Food supplement exclusion")
    enriched = enrich_food_signal(record)
    if enriched.get("is_noise") or enriched.get("dashboard_section") == "excluded":
        raise ValueError("Excluded by Food quality/relevance rules")
    return raw


def candidates(conn, source, rows, lo, hi):
    good, held, ids, urls = [], [], defaultdict(set), defaultdict(set)
    for row in rows:
        if isinstance(row,dict) and isinstance(row.get("source_id"),str) and isinstance(row.get("url"),str):
            ids[row["source_id"]].add(json.dumps(row,sort_keys=True))
            urls[row["url"]].add(row["source_id"])
    seen = set()
    duplicates = 0
    for row in rows:
        ident = row.get("source_id") if isinstance(row,dict) else None
        try:
            item = validated(source, row, lo, hi)
            if len(ids[ident]) > 1 or len(urls[row["url"]]) > 1:
                raise ValueError("Conflicting source identities")
            old = conn.execute("SELECT domain,coalesce(nullif(source_label,''),authority),url FROM signals WHERE source_id=?", (ident,)).fetchone()
            if old and tuple(old) != ("food" if source in FOOD else "vms", source, row["url"]):
                raise ValueError("Stored identity conflict")
            if ident in seen:
                duplicates += 1
                continue
            seen.add(ident)
            good.append(item)
        except (ValueError,TypeError,KeyError,AttributeError) as exc:
            held.append({"id":ident, "reason":str(exc)[:120]})
    return good, held, duplicates


def process(db, state, folder, source, ticket, publish, reviewed=False):
    path = folder/(source+".json")
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != ticket["sha256"]:
        raise ValueError("Manifest changed after collection")
    m = json.loads(content)
    lo, hi = (date.fromisoformat(ticket[k]) for k in ("since","until"))
    coverage = source_gate(source,m,ticket["exit"],lo,hi)
    result = dict(source=source, since=str(lo), until=str(hi), coverage=coverage, collected=len(m["rows"]),
                  checked_at=m["checked_at"], run=folder.name)
    with closing(connect(db, "rw" if publish else "ro")) as live, closing(sqlite3.connect(":memory:")) as scratch:
        conn = live if publish else scratch
        if not publish:
            live.backup(scratch)
        conn.row_factory = sqlite3.Row
        with conn:
            conn.execute("BEGIN IMMEDIATE")
            prior = [r for r in receipts(conn) if r["source"] == source]
            if prior and prior[-1]["run"] == folder.name:
                return dict(prior[-1], status="already_published")
            if prior and prior[-1]["checked_at"] >= result["checked_at"]:
                raise ValueError("Superseded manifest; collect again")
            good, held, dup = candidates(conn,source,m["rows"],lo,hi)
            result["candidate_digest"] = hashlib.sha256(json.dumps(good,sort_keys=True).encode()).hexdigest()
            if reviewed:
                preview = read(folder/(source+".preview.json"))
                if preview.get("status") != "preview" or preview.get("candidate_digest") != result["candidate_digest"]:
                    raise ValueError("No matching successful preview; re-preview the selected source")
            save(folder/(source+".holds.json"), held)
            if publish:
                # Writer reservation makes this the exact pre-transaction image.
                result["backup"] = str(backup(db,state/"backups"))
            # Protect original IDs and records even if future merge logic regresses.
            # source is a registry constant, not remote text.
            domain = "food" if source in FOOD else "vms"
            for op, condition in (
                ("DELETE","1"),
                ("UPDATE",f"OLD.domain IS NOT '{domain}' OR coalesce(nullif(OLD.source_label,''),OLD.authority) IS NOT '{source}' OR OLD.id IS NOT NEW.id OR OLD.source_id IS NOT NEW.source_id OR OLD.created_at IS NOT NEW.created_at OR OLD.domain IS NOT NEW.domain OR OLD.source_label IS NOT NEW.source_label OR OLD.authority IS NOT NEW.authority"),
                ("INSERT",f"NEW.domain IS NOT '{domain}' OR NEW.source_label IS NOT '{source}'"),
            ):
                conn.execute(f"CREATE TEMP TRIGGER scope_{op} BEFORE {op} ON main.signals WHEN {condition} BEGIN SELECT RAISE(ABORT,'out-of-scope signal write'); END")
            result.update(counts=dict(merge(conn,source,good)), eligible=len(good), held=len(held),
                          repeated=dup, status=("published" if good else "review_only" if held else "verified_empty")
                          if publish else "preview", finished_at=utc())
            if reviewed and result["counts"] != preview["counts"]:
                raise ValueError("Merge effects differ from preview; rolling back for review")
            if publish:
                conn.execute(f"CREATE TABLE IF NOT EXISTS {TABLE}(run TEXT, source TEXT, result TEXT NOT NULL, PRIMARY KEY(run,source))")
                conn.execute(f"INSERT INTO {TABLE} VALUES(?,?,?)",(folder.name,source,json.dumps(result)))
            return result


def run(db, state, sources, start, end, daily=False, publish=False, replay=None, collector=collect):
    if publish and os.getenv(ENABLE,"").lower() != "true":
        raise ValueError(f"Publication disabled: set {ENABLE}=true after validation")
    if not sources or not set(sources) <= SOURCES.keys() or start > end or end > datetime.now(timezone.utc).date():
        raise ValueError("Invalid source or interval")
    db = db.resolve(strict=True)
    with exclusive(db):
        state.mkdir(mode=0o700, parents=True, exist_ok=True)
        binding = state/"target.json"
        target = {"db":str(db),"device":db.stat().st_dev,"inode":db.stat().st_ino}
        if not binding.exists():
            save(binding,target)
        if read(binding) != target:
            raise ValueError("State belongs to another database")
        with closing(connect(db)) as c:
            prior = receipts(c)
        if replay:
            if not re.fullmatch(r"\d{8}T\d{12}Z-[a-f0-9]{8}",replay):
                raise ValueError("Invalid staged run ID")
            folder = state/replay
            plan = read(folder/"plan.json")
            if not set(sources) <= set(plan):
                raise ValueError("Source not in staged run")
        else:
            folder = state/(datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")+"-"+uuid4().hex[:8])
            folder.mkdir(mode=0o700)
            plan = {s:list(map(str,next_window(s,start,end,daily,prior))) for s in sources}
            save(folder/"plan.json",plan)
        print("run:",folder.name,flush=True)
        incomplete = False
        for s in sources:
            lo,hi = map(date.fromisoformat,plan[s])
            report = folder/(s+(".apply.json" if publish else ".preview.json"))
            started = utc()
            save(report,dict(source=s,run=folder.name,status="in_progress_or_interrupted",since=str(lo),
                             until=str(hi),started_at=started,finished_at=started,coverage="not_verified"))
            try:
                ticket_path = folder/(s+".ticket.json")
                if not replay:
                    code = collector(s,folder,db,lo,hi)
                    digest = hashlib.sha256((folder/(s+".json")).read_bytes()).hexdigest()
                    save(ticket_path,dict(exit=code,sha256=digest,since=str(lo),until=str(hi)))
                ticket = read(ticket_path)
                if (ticket["since"],ticket["until"]) != tuple(plan[s]):
                    raise ValueError("Ticket does not match staged plan")
                r = process(db,state,folder,s,ticket,publish,reviewed=bool(publish and replay))
            except (ValueError,KeyError,TypeError,OSError,sqlite3.Error,subprocess.TimeoutExpired) as exc:
                r = dict(source=s,run=folder.name,status="failed",coverage="not_verified",since=str(lo),until=str(hi),
                         failure=str(exc)[:200],finished_at=utc(),counts={"new":0,"updated":0},collected=None)
                try:
                    raw = read(folder/(s+".json"))
                    r.update(collected=len(raw["rows"]),held=len(raw["rows"]),checked_at=raw["checked_at"])
                except (ValueError,OSError,KeyError,TypeError):
                    pass
            r["started_at"] = started
            r["backlog"] = daily and hi < end
            save(report,r)
            print(json.dumps(r),flush=True)
            incomplete |= r["status"] == "failed" or r["coverage"] != "query_window_verified" or r["backlog"] or bool(r.get("held"))
        return 2 if incomplete else 0


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("mode",choices=("audit","full","daily","status"))
    p.add_argument("--db",required=True,type=Path)
    p.add_argument("--state",type=Path)
    p.add_argument("--before",type=Path,help="Audit: verify existing identities and out-of-scope rows against this backup")
    p.add_argument("--source",action="append",choices=SOURCES)
    p.add_argument("--since",type=date.fromisoformat)
    p.add_argument("--until",type=date.fromisoformat)
    group = p.add_mutually_exclusive_group()
    group.add_argument("--publish",action="store_true")
    group.add_argument("--configured",action="store_true",help="Timer mode: preview unless publication enabled")
    p.add_argument("--replay",help="Use the exact previously staged run; no network collection")
    a = p.parse_args()
    os.umask(0o077)
    if a.mode == "audit":
        if a.before:
            verify_baseline(a.db,a.before)
            print("Verified original identities and unchanged out-of-scope signals")
        print(json.dumps(snapshot(a.db)[1],indent=2))
        return 0
    if not a.state:
        p.error("--state required")
    if a.mode == "status":
        with closing(connect(a.db)) as c:
            prior = receipts(c)
            last = {r["source"]:r for r in prior}
            published = {r["source"]:r for r in prior if r["status"] == "published"}
        attempts, failures = {}, {}
        binding = a.state/"target.json"
        if binding.exists() and read(binding) != {"db":str(a.db.resolve()),"device":a.db.stat().st_dev,"inode":a.db.stat().st_ino}:
            p.error("State belongs to another database")
        for f in sorted(a.state.glob("*/*.json")):
            if f.name.endswith((".preview.json",".apply.json")):
                r = read(f)
                if r["finished_at"] >= attempts.get(r["source"],{}).get("finished_at",""):
                    attempts[r["source"]] = r
                if r["status"] == "failed" and r["finished_at"] >= failures.get(r["source"],{}).get("finished_at",""):
                    failures[r["source"]] = r
        print(json.dumps({s:{"last_publication":published.get(s),"last_committed_check":last.get(s),"last_attempt":attempts.get(s),
                             "last_failure":failures.get(s),
                             "unverified_intervals":gaps(s,a.since,a.until or datetime.now(timezone.utc).date()-timedelta(days=1),prior)
                             if a.since else None} for s in a.source or SOURCES},indent=2))
        return 0
    if not a.since and not a.replay:
        p.error("--since must be the first day to cover")
    end = a.until or datetime.now(timezone.utc).date()-timedelta(days=1)
    try:
        return run(a.db,a.state,a.source or list(SOURCES),a.since or end,end,a.mode=="daily",
                   a.publish or (a.configured and os.getenv(ENABLE,"").lower()=="true"),a.replay)
    except (OSError,ValueError) as exc:
        p.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
