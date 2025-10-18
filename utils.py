import re
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode
from datetime import datetime
from bs4 import BeautifulSoup
from config import ABS_BASE

def normalize_url(u: str) -> str:
    try:
        p = urlparse(u)
        q = parse_qs(p.query, keep_blank_values=True)
        items = [(k, v) for k in sorted(q.keys()) for v in sorted(q[k])]
        new_q = urlencode(items)
        return urlunparse((p.scheme, p.netloc, p.path, "", new_q, ""))
    except Exception:
        return (u or "").strip()

def absolutize_url(v: str) -> str:
    v = (v or "").strip()
    return ABS_BASE + v if v.startswith("/") else v

def parse_row_date(text: str):
    m = re.search(r"\d{4}[.\-]\d{2}[.\-]\d{2}", text or "")
    if not m: return None
    s = m.group().replace(".", "-")
    return datetime.strptime(s, "%Y-%m-%d").date()

def fix_relative_urls_in_soup(soup: BeautifulSoup):
    """
    <img>, <a> 태그의 상대경로를 절대경로로 변환
    이미 절대경로인 경우는 그대로 유지
    """
    for tag in soup.find_all(["a", "img"]):
        attr = "href" if tag.name == "a" else "src"
        if not tag.has_attr(attr):
            continue
        val = (tag.get(attr) or "").strip()
        if val and not urlparse(val).netloc:  # netloc 없으면 상대경로
            tag[attr] = absolutize_url(val)