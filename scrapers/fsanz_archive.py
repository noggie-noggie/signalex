"""Walk FSANZ's dated listing cards; never follow external navigation links."""
import re
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup


def published(node):
    if node is not None:
        text = node.get_text(" ", strip=True).removeprefix("Published ").strip()
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                pass
    return None


def listings(session, url, since, date_selector):
    page = url
    seen = set()
    for index in range(50):
        if page in seen:
            raise ValueError("Repeated FSANZ pagination link")
        seen.add(page)
        if index:
            time.sleep(1)
        resp = session.get(page, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "lxml")
        yield soup
        if since is None:
            return
        dates = [published(n) for n in soup.select(date_selector)]
        if not dates or None in dates:
            raise ValueError("FSANZ listing dates missing or unrecognised")
        if max(dates).date() < since:
            return
        nxt = soup.select_one('.pager a[rel~=next], a[rel~=next], .pager__item--next a')
        if nxt is None:
            return
        target = urlsplit(urljoin(page, nxt.get("href", "")))
        base = urlsplit(url)
        if (target.scheme, target.netloc, target.path) != (base.scheme, base.netloc, base.path) or not re.fullmatch(r"page=\d+", target.query):
            raise ValueError("Unexpected FSANZ archive pagination")
        page = target.geturl()
    raise ValueError("FSANZ archive limit reached")


def detail_summary(session, url):
    parts = urlsplit(url)
    if (parts.scheme, parts.netloc) != ("https", "www.foodstandards.gov.au"):
        raise ValueError("Unexpected FSANZ notice origin")
    response = session.get(url, timeout=30)
    response.raise_for_status()
    page = BeautifulSoup(response.content, "lxml")
    paragraphs = [" ".join(p.stripped_strings) for p in page.select("main .field-bare p")]
    if not any(paragraphs):
        raise ValueError("Missing FSANZ detail paragraphs")
    return " ".join(p for p in paragraphs[:4] if p)[:1000]
