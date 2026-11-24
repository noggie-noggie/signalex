"""Check every installed distribution against public PyPI version-specific advisories."""
from concurrent.futures import ThreadPoolExecutor
from importlib.metadata import distributions
import requests


def check(dist):
    name, version = dist.metadata["Name"], dist.version
    try:
        response = requests.get(f"https://pypi.org/pypi/{name}/{version}/json", timeout=20)
        response.raise_for_status()
        data = response.json()
        issues = sorted(v["id"] for v in data["vulnerabilities"] if not v.get("withdrawn"))
        if data["info"].get("yanked"):
            issues.append("YANKED")
        return name, version, issues
    except (requests.RequestException, KeyError, ValueError) as exc:
        return name, version, ["unverified:" + type(exc).__name__]


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = sorted(pool.map(check, distributions()))
    for name, version, issues in results:
        if issues:
            print(name, version, *issues)
    count = sum(bool(issues) for _, _, issues in results)
    print(f"Checked {len(results)} installed packages; {count} flagged or unverified")
    raise SystemExit(bool(count))
