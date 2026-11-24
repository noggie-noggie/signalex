# Signalex Food/VMS maintenance

One CLI: `python -m maintenance.refresh {audit,full,daily,status}`. Run on Linux/WSL with an existing SQLite file on a local filesystem supporting flock. **No WSL database, manifests or cursors go to Vultr.** Only reviewed code goes there. No Pharma jobs, model calls, email or Slack. Never run the legacy scheduler alongside this publisher.

| Mode | Effect |
|---|---|
| audit | Read-only local counts/quality. With `--before BACKUP`, verify original identities and byte-equivalent values of all out-of-scope signal rows. |
| full | Collect all three Food and all registered VMS sources. Stage private raw manifests, validate provenance/domain and classify each row. Default: rehearse the actual merges against in-memory copies, without writing the target. |
| daily | Same checks. Verified archives catch up in contiguous intervals from the requested start, up to seven days per invocation, and revisit the latest day once caught up. Snapshots revisit recent observations. A failed source does not advance its published coverage. |
| status | Read-only: per-source last committed receipt, latest attempt/failure, counts, holds, backup and timestamps. Add `--since` and optionally `--until` for unverified UTC intervals. |

`--source NAME` is repeatable. `--publish` requires `SIGNALEX_REFRESH_PUBLISH=true`; `--configured` previews unless that setting is true. Do **not** enable the timer's publication setting until the production preview, scoped manual promotions and verification below pass. `--replay RUN_ID` uses the exact staged manifest digest for the same database and does not collect again. State is private and bound to database path and file identity. Back up the state/hold manifests along with SQLite. Do not edit tickets.

Publication is per source, not all-or-nothing across the system. A failed/partial fetch (including a failed OFF category) writes **nothing** from that source. Validated FSANZ updates and Australian OFF catalogue observations from clean snapshots can publish without declaring historical coverage. The FSANZ recall listing can prove a bounded listing window. Ambiguous IDs, poor quality, supplements and VMS claims without a factual source classification stay in review. Raw observations are not proof of a regulatory decision or a launch date. Duplicate source IDs, FSANZ aliases and OFF category families are handled before/API-side or during the same source transaction; old records are not deleted. Each write transaction takes a verified pre-write SQLite backup under a writer reservation and commits its receipt with the data. Keep enough free disk space for these backups and arrange off-host retention.

Exit **0** = selected intervals processed without reported gaps; **2** = inspect status (holds, partial coverage, backlog, source failure or CLI/configuration error). A nonzero run may have safely committed other sources. Do not treat a whole run as green simply because an individual source published. Raw/hold/attempt files live under `STATE/RUN_ID/`. A timeout/abrupt stop before a receipt does not advance a source; retry. No autonomous reviewer or notifier is installed.

## 1. WSL: regression only, no refresh

```bash
set -euo pipefail
cd /mnt/c/Users/in-qo/Desktop/vms-intel
P=.venv/bin/python
"$P" -m unittest test_api_backend.py test_food_*.py test_refresh.py test_maintenance_refresh.py
"$P" -m compileall -q maintenance scrapers services
git diff --check
git status --short
# For a later manual commit: review tracked AND untracked scripts/tests/docs, stage
# only the reviewed backend paths, then inspect git diff --cached before committing.
```

## 2. Vultr: prepare and rehearse with the API still serving

Use one Bash session as the deployment owner. These examples assume the API service user is `signalex`, plain Uvicorn on **127.0.0.1:8000**, the live data under `/srv/signalex/data`, and existing systemd authentication/CORS and Nginx configuration. Inspect the actual unit and private environment locally; if flags, workers, socket activation or users differ, adapt and verify the drop-in before proceeding. Use an approved production commit. Pause legacy ingestion writers/old staging timer and confirm no refresh is already running; do not stop the API. The CLI locks out concurrent instances, including ones using another state directory.

```bash
set -euo pipefail; umask 077
LIVE=/srv/signalex; DB="$LIVE/data/signals.db"; ROOT=/srv/signalex-runtime
STATE=/var/lib/signalex-stage/refresh
cd "$LIVE"; test -s "$DB"
read -rp 'Confirmed API service (including .service): ' UNIT
[[ $UNIT =~ ^[a-zA-Z0-9@_.-]+\.service$ ]]
systemctl is-active --quiet "$UNIT"
systemctl show "$UNIT" -p User -p WorkingDirectory -p FragmentPath
systemctl list-timers --all
sudo -u signalex test -r "$DB"; sudo -u signalex test -w "$LIVE/data"
git fetch origin
read -rp 'Reviewed full release SHA: ' REV
test "$(git rev-parse "$REV^{commit}")" = "$REV"; git merge-base --is-ancestor "$REV" origin/main
# ROOT and releases must be trusted deployment-owned directories, readable but not writable by the API service.
sudo install -d -o "$(id -un)" -m 0755 "$ROOT" "$ROOT/releases"
R="$ROOT/releases/$REV"; test ! -e "$R"; mkdir -m 0755 "$R" "$R/app"
mkdir -p "$HOME/signalex-maintenance"
RUN=$(mktemp -d "$HOME/signalex-maintenance/$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX"); P="$R/bin/python"
(umask 022; git archive "$REV" | tar -x -C "$R/app" --exclude='reports/*'
 python3.12 -m venv "$R"
 "$P" -m pip install 'pip>=26.2'
 "$P" -m pip install -r "$R/app/requirements.txt" fastapi==0.136.1 uvicorn==0.47.0)
cd "$R/app"; "$P" -m pip check; "$P" -m maintenance.advisories
"$P" -m maintenance.reconcile --db "$DB" --backup-dir "$RUN/baseline"
BEFORE=$(find "$RUN/baseline" -name '*.db' -type f); test -s "$BEFORE"
mkdir -p data reports; cp --no-clobber "$BEFORE" data/signals.db
ln -s "$LIVE/reports/citation_database.json" reports/citation_database.json
"$P" -m unittest test_api_backend.py test_food_*.py test_refresh.py test_maintenance_refresh.py
# Candidate process uses ONLY its copy and a throwaway credential. No paid AI.
(
 export APP_ENV=production CORS_ORIGINS=https://partner.example FOOD_CLAIM_REVIEW_AI_ENABLED=false
 SIGNALEX_API_KEY=$(openssl rand -hex 32); export SIGNALEX_API_KEY
 "$P" -m uvicorn api.server:app --host 127.0.0.1 --port 0 >"$RUN/candidate.log" 2>&1 & child=$!
 trap 'kill "$child" 2>/dev/null || :; wait "$child" 2>/dev/null || :' EXIT
 for i in {1..60}; do grep -q 'Uvicorn running' "$RUN/candidate.log" && break; sleep .25; done
 URL=$(grep -oE 'http://127\.0\.0\.1:[0-9]+' "$RUN/candidate.log" | head -1)
 "$P" -m maintenance.smoke "$URL" --origin "$CORS_ORIGINS"
)
# The candidate's test DB is moved aside, NEVER copied back to production.
mv data "$RUN/candidate-data"
ln -s "$LIVE/data" data
test "$(readlink -f data/signals.db)" = "$DB"
if test -f "$LIVE/.env"; then ln -s "$LIVE/.env" .env; fi
chmod 0755 reports; sudo -u signalex test -r reports/citation_database.json
"$P" -m maintenance.refresh audit --db "$DB" --before "$BEFORE"
```

## 3. Vultr: collect Food **and** VMS once; preview publication

Still no API stop and no signal writes. Use the actual production gap, not a WSL cursor. Run as the timer user so the same private state can be reused. For all subsequent production commands use this wrapper from the candidate/release app directory. Exit 2 requires inspection; it is not approval to publish a failed source.

```bash
SINCE=2026-08-24; UNTIL=$(date -u -d yesterday +%F)
sudo install -d -o signalex -g "$(id -gn signalex)" -m 0700 /var/lib/signalex-stage "$STATE"
maint() { sudo -u signalex env SIGNALEX_REFRESH_PUBLISH="${ALLOW_PUBLISH:-false}" "$P" -m maintenance.refresh "$1" --db "$DB" --state "$STATE" "${@:2}"; }
ALLOW_PUBLISH=false
rc=0; maint full --since "$SINCE" --until "$UNTIL" | tee "$RUN/preview.log" || rc=$?
test "$rc" -eq 0 || test "$rc" -eq 2
ID=$(sed -n 's/^run: //p' "$RUN/preview.log"); test -n "$ID"
maint status --since "$SINCE" --until "$UNTIL"
sudo -u signalex ls "$STATE/$ID"
# Review each .preview.json, .holds.json and source samples. All three Food
# manifests (food_fsanz_recalls, food_fsanz_updates, open_food_facts) belong here.
# Keep the API online; do not release until candidate and scoped previews pass.
```

## 4. Vultr: code cutover and rollback

No live checkout merge, service stop, dependency installation or data promotion is needed here. A versioned runtime contains both code and dependencies. Preserve the old runtime and only our own unit drop-in. Leave the staging timer paused until verification finishes. Complete all preflight checks before replacing the active pointer. On a switch or readiness failure the shell restores the old pointer/drop-in and restarts the previous service. An interactive SIGKILL/power loss cannot execute shell traps; keep the saved rollback location.

```bash
cd "$R/app"; systemctl is-active --quiet "$UNIT"
test -x "$P"; test -r api/server.py
CURRENT="$ROOT/current"; DROP="/etc/systemd/system/$UNIT.d/90-signalex-release.conf"
test ! -e "$CURRENT" || test -L "$CURRENT"
readlink "$CURRENT" > "$RUN/old-link" || :
if sudo test -f "$DROP"; then sudo cat "$DROP" > "$RUN/old-drop"; fi
printf '[Service]\nWorkingDirectory=%s/current/app\nExecStart=\nExecStart=%s/current/bin/python -m uvicorn api.server:app --host 127.0.0.1 --port 8000\n' "$ROOT" "$ROOT" > "$RUN/new-drop"
swap_link() { local temp; temp=$(mktemp -d "$ROOT/switch-XXXXXX"); ln -s "$1" "$temp/next"; mv -Tf "$temp/next" "$CURRENT"; rmdir "$temp"; }
rollback() {
 if test -s "$RUN/old-link"; then swap_link "$(cat "$RUN/old-link")"; elif test -L "$CURRENT"; then unlink "$CURRENT"; fi
 if test -f "$RUN/old-drop"; then sudo install -m 0644 "$RUN/old-drop" "$DROP"; elif sudo test -f "$DROP"; then sudo unlink "$DROP"; fi
 sudo systemctl daemon-reload; sudo systemctl restart "$UNIT"; systemctl is-active "$UNIT"
}
printf 'rollback state: %s; old pointer: %s\n' "$RUN" "$(cat "$RUN/old-link")"
read -rp 'Candidate checks passed; restart only the verified API? Type SWITCH: ' answer; test "$answer" = SWITCH
(
 set -e
 read -rsp 'Backend API key for smoke: ' SIGNALEX_API_KEY; echo; export SIGNALEX_API_KEY
 read -rp 'Trusted partner HTTPS origin: ' ORIGIN
 test -n "$SIGNALEX_API_KEY"; test -n "$ORIGIN"
 trap 'rc=$?; if ((rc)); then rollback; fi' EXIT
 trap 'exit 130' INT TERM
 sudo mkdir -p "$(dirname "$DROP")"
 swap_link "$R"; sudo install -m 0644 "$RUN/new-drop" "$DROP"
 sudo systemctl daemon-reload; sudo systemctl restart "$UNIT"
 for i in {1..30}; do curl -fsS http://127.0.0.1:8000/api/health >"$RUN/health" && break; sleep 1; done
 "$P" -c 'import json,sys; assert json.load(open(sys.argv[1]))["status"]=="ok"' "$RUN/health"
 "$P" -m maintenance.smoke https://api.signalex.com.au --origin "$ORIGIN"
)
# If a later code check fails in this session: rollback
```

## 5. Vultr: scoped production data promotion, verify, then opt into automation

Apply only reviewed scopes from the **same Vultr preview**. The API stays up. No operation replaces the database. For example, enter `food_fsanz_recalls food_fsanz_updates open_food_facts pubmed` **only if** those previews/evidence pass. Repeat for independently approved sources; never approve a failed collection or certify snapshot history.

```bash
cd "$R/app"
read -rp 'Space-separated approved source names from the preview: ' -a approved
test "${#approved[@]}" -gt 0
args=(); for source in "${approved[@]}"; do args+=(--source "$source"); done
ALLOW_PUBLISH=true
rc=0; maint full --replay "$ID" --publish "${args[@]}" || rc=$?
ALLOW_PUBLISH=false
test "$rc" -eq 0 || test "$rc" -eq 2
maint status --since "$SINCE" --until "$UNTIL"
"$P" -m maintenance.refresh audit --db "$DB" --before "$BEFORE"
# Inspect receipts, stored identities, visible Food sections, samples and VMS counts.
# If verification fails: leave publication disabled. See rollback instructions below.
```

If the candidate set or merge counts differ from the approved preview, that source transaction rolls back. Use `maint full --replay "$ID" --source NAME` with publication disabled to re-preview, then review the new counts; if a newer manifest already committed, collect a fresh scoped preview instead. A run that only advanced coverage or held rows is not a publication of those rows.

If the prior VMS historical text repair also remains outstanding on Vultr, run `"$P" -m maintenance.vms_history --db "$DB"` separately, preview only. Apply only after separate approval using its explicit `--apply --backup-dir "$RUN/history-checkpoints"` controls, with refresh writers paused. It is not part of daily collection.

Install the timer in **preview** mode first. It uses the same CLI, database lock, source receipts and private state as the manual operator. Keep the API unit's existing secret, CORS and environment; the maintenance unit doesn't need an AI key.

```bash
cd "$R/app"
sudo install -m 0644 maintenance/systemd/signalex-stage.{service,timer} /etc/systemd/system/
printf 'FIRST_DAY=%s\nSIGNALEX_REFRESH_PUBLISH=false\n' "$SINCE" | sudo tee /etc/signalex-stage.conf >/dev/null
sudo chmod 0600 /etc/signalex-stage.conf
sudo systemd-analyze verify /etc/systemd/system/signalex-stage.{service,timer}
sudo systemctl daemon-reload; sudo systemctl enable --now signalex-stage.timer
systemctl list-timers signalex-stage.timer --all
journalctl -u signalex-stage.service -n 40 --no-pager
# ONLY after production validation and operator approval:
# sudo sed -i 's/^SIGNALEX_REFRESH_PUBLISH=false$/SIGNALEX_REFRESH_PUBLISH=true/' /etc/signalex-stage.conf
# Setting is read at the next invocation. Revert to false to disable future publication.
```

**Data rollback is separate from code rollback.** Disable the timer and pause other ingestion writers. Keep the failed state, source receipts and their exact pre-write backups. Check which source transactions committed; a failed transaction itself needs no undo. Prefer a reviewed correction/quarantine by original ID. For disaster recovery, only with explicit approval, quiesce **all** SQLite writers (including API cache writes), take a fresh forensic SQLite backup, verify the chosen Vultr backup's integrity and reconcile intervening writes. Restore using SQLite's backup/restore interface with exclusive access, not a file copy over a live SQLite/WAL. Verify identities, out-of-scope rows and HTTP health before reopening writers. Rebind new state deliberately if the DB file identity changes. Never use a local DB, and never regenerate Pharma.
