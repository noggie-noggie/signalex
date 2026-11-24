"""Archive pagination; fail closed on missing, repeated or changing results."""
from xml.etree import ElementTree as ET
from datetime import datetime, timedelta


def europe_pages(session, url, params):
    cursor, seen, keys, total = "*", set(), set(), None
    while cursor not in seen:
        seen.add(cursor)
        # Core records include abstracts. Smaller pages avoid repeating large responses after timeouts.
        resp = session.get(url, params=dict(params, cursorMark=cursor, pageSize=25), timeout=(10,60))
        resp.raise_for_status()
        data = resp.json()
        count = int(data["hitCount"])
        if total is not None and total != count:
            raise ValueError("Europe PMC count changed")
        total = count
        for r in data["resultList"]["result"]:
            key = r["source"], r["id"]
            if key in keys or not r.get("title"):
                raise ValueError("Repeated or incomplete Europe PMC result")
            keys.add(key)
            yield r
        if len(keys) == total:
            return
        cursor = data.get("nextCursorMark")
        if not cursor or len(keys) > total:
            break
    raise ValueError("Europe PMC cursor/count did not reconcile")


def trial_pages(session, url, params):
    seen, tokens, total = set(), set(), None
    params = dict(params, pageSize=100, countTotal="true")
    while True:
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if total is None:
            total = int(data["totalCount"])
        elif int(data.get("totalCount", total)) != total:
            raise ValueError("Trial result count changed; repeat this window")
        studies = data["studies"]
        for study in studies:
            key = study["protocolSection"]["identificationModule"]["nctId"]
            if not key or key in seen:
                raise ValueError("Repeated or missing trial identifier")
            seen.add(key)
            yield study
        token = data.get("nextPageToken")
        if not token:
            if len(seen) != total:
                raise ValueError("Trial pagination did not reconcile")
            return
        if not studies or token in tokens:
            raise ValueError("Invalid trial cursor")
        tokens.add(token)
        params = dict(params, pageToken=token)


def pmids(session, url, params):
    ids, seen, total = [], set(), None
    while True:
        response = session.get(url, params=dict(params, retstart=len(ids), retmax=250), timeout=30)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        count = int(root.findtext("Count"))
        if count > 9999:
            start, end = [datetime.strptime(params[k], "%Y/%m/%d").date() for k in ("mindate", "maxdate")]
            if start >= end:
                raise ValueError("Single PubMed day exceeds retrieval limit")
            mid = start + (end-start)//2
            left = pmids(session, url, dict(params, maxdate=mid.strftime("%Y/%m/%d")))
            right = pmids(session, url, dict(params, mindate=(mid+timedelta(days=1)).strftime("%Y/%m/%d")))
            merged = list(dict.fromkeys(left + right))
            if len(merged) != count:
                raise ValueError("Subdivided PubMed interval did not reconcile")
            return merged
        if root.find(".//ERROR") is not None or (total is not None and count != total):
            raise ValueError("PubMed query error, count changed, or interval needs subdivision")
        total = count
        batch = [e.text for e in root.findall("IdList/Id")]
        if any(not i or i in seen for i in batch) or len(set(batch)) != len(batch):
            raise ValueError("Invalid PubMed page")
        ids.extend(batch)
        seen.update(batch)
        if len(ids) == total:
            return ids
        if not batch or len(ids) > total:
            raise ValueError("PubMed pagination did not reconcile")
