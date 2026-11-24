"""HTTP contract checks, using an environment key and a deterministic-only claim."""
import argparse
import os

import requests


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("base")
    p.add_argument("--origin", required=True)
    args = p.parse_args()
    base = args.base.rstrip("/")
    key = os.environ["SIGNALEX_API_KEY"]
    if not key or not (base.startswith("https://") or base.startswith("http://127.0.0.1:")):
        p.error("Use HTTPS or local loopback and a nonempty key")
    def check(method, path, status=200, **kwargs):
        r = requests.request(method, base+path, timeout=20, allow_redirects=False, **kwargs)
        if r.status_code != status:
            raise RuntimeError(f"{method} {path}: expected {status}, got {r.status_code}")
        print(method, path, r.status_code)
        return r
    auth = {"x-api-key":key}
    if check("GET","/api/health").json().get("status") != "ok":
        raise RuntimeError("Health degraded: inspect DB and required files")
    check("GET", "/api/signals", 401)
    check("GET", "/api/signals", 403, headers={"x-api-key":"invalid-test-key"})
    for domain in ("food","vms"):
        data = check("GET",f"/api/signals?domain={domain}&limit=3",headers=auth).json()
        if not {"results","total","limit","offset"} <= data.keys() or not data["results"]:
            raise RuntimeError(f"Empty or incompatible {domain} feed")
        for row in data["results"]:
            if row["domain"] != domain or (domain == "food" and not
                {"dashboard_section","impact","momentum","category","product_type","issue_area","claim_theme"} <= row.keys()):
                raise RuntimeError("Signal schema/domain mismatch")
    for path in ("meta","food/dashboard","food/recalls?limit=1","food/products?limit=1",
                 "food/rules?limit=1","food/claim-pathways?claim=high_protein"):
        check("GET","/api/"+path,headers=auth)
    preflight = {"Origin":args.origin,"Access-Control-Request-Method":"POST",
                 "Access-Control-Request-Headers":"x-api-key,content-type"}
    cors = check("OPTIONS","/api/food/claim-review",headers=preflight)
    if cors.headers.get("access-control-allow-origin") != args.origin:
        raise RuntimeError("Expected partner origin not allowed")
    check("OPTIONS","/api/food/claim-review",400,headers={**preflight,"Origin":"https://untrusted.invalid"})
    card = check("POST","/api/food/claim-review",headers=auth,json={"claim_text":"supports IBS",
                 "food_type":"yoghurt","use_ai":False}).json()
    if card["risk_level"] != "high" or card["ai_used"]:
        raise RuntimeError("Unexpected deterministic claim review")


if __name__ == "__main__":
    main()
