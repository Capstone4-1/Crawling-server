from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from datetime import datetime, date
from urllib.parse import urlparse, parse_qs
from config import ABS_BASE, TIMEOUT_SEC

def absolutize_url(v: str) -> str:
    v = (v or "").strip()
    return ABS_BASE + v if v.startswith("/") else v

def extract_nttSn_from_any(s: str):
    import re
    try:
        from urllib.parse import urlparse, parse_qs
        q = parse_qs(urlparse(s).query)
        if q.get("nttSn"): return q["nttSn"][0]
    except Exception:
        pass
    for p in (r"selectNttInfo\(['\"]?(\d+)['\"]?\)",
              r"fn_egov_select_nttInfo\(['\"]?(\d+)['\"]?\)",
              r"viewNtt\(['\"]?(\d+)['\"]?\)",
              r"nttSn\s*=\s*['\"]?(\d+)['\"]?"):
        m = re.search(p, s or "")
        if m: return m.group(1)
    return None

def build_detail_url(href: str, onclick: str):
    href = (href or "").strip()
    if href and not href.startswith("#") and not href.lower().startswith("javascript"):
        return absolutize_url(href)
    ntt = extract_nttSn_from_any(onclick or "")
    if ntt:
        return f"{ABS_BASE}/kmou/na/ntt/selectNttInfo.do?nttSn={ntt}&mi=2032"
    return None

def scan_list_page(driver):
    """목록 페이지에서 글 정보 수집"""
    soup = BeautifulSoup(driver.page_source, "html.parser")
    rows = soup.select("tbody tr")
    items = []
    for tr in rows:
        a = (tr.select_one("a[href*='selectNttInfo']") or
             tr.select_one("a[onclick*='nttSn']") or
             tr.select_one("a"))
        if not a:
            continue

        title = (a.get_text(strip=True) or a.get("title") or "").strip()
        detail_url = build_detail_url(a.get("href",""), a.get("onclick",""))
        if not detail_url:
            continue

        row_txt_all = tr.get_text(" ", strip=True)
        sticky = any(k in row_txt_all for k in ("공지", "Notice", "NOTICE", "TOP", "Top"))

        ntt_sn = extract_nttSn_from_any(detail_url) or extract_nttSn_from_any(a.get("onclick",""))
        items.append({
            "title": title,
            "detail_url": detail_url,
            "is_sticky": sticky,
            "ntt_sn": ntt_sn
        })
    return items

def fix_relative_urls_in_soup(soup: BeautifulSoup):
    for tag in soup.find_all(["a", "img"]):
        attr = "href" if tag.name == "a" else "src"
        if not tag.has_attr(attr): continue
        tag[attr] = absolutize_url(tag.get(attr) or "")

def pick_thumbnail_from_content(html: str):
    s = BeautifulSoup(html, "html.parser")
    for img in s.select("img"):
        src = (img.get("src") or img.get("data-src") or "").strip()
        if not src: continue
        src = absolutize_url(src)
        low = src.lower()
        if any(x in low for x in ["blank.gif", "spacer", "icon", "ico_", "/ico/", "sprite", "emoticon", "emoji", "logo"]):
            continue
        if low.endswith(".svg"):
            continue
        return src
    return None

def parse_detail_date_from_html(content_html: str) -> date | None:
    """본문 HTML에서 '등록일' 혹은 날짜 문자열 추출"""
    try:
        soup = BeautifulSoup(content_html, "html.parser")
        # '등록일' 텍스트를 가진 <th> 또는 <dt>를 찾음
        label = soup.find(lambda t: t.name in ("th","dt") and "등록일" in t.get_text(strip=True))
        if label:
            # 바로 다음 형제에서 날짜 문자열 추출
            sibling = label.find_next_sibling()
            if sibling:
                text = sibling.get_text(" ", strip=True)
                # YYYY.MM.DD 또는 YYYY-MM-DD
                import re
                m = re.search(r"\d{4}[.-]\d{2}[.-]\d{2}", text)
                if m:
                    s = m.group().replace(".", "-")
                    return datetime.strptime(s, "%Y-%m-%d").date()
        # fallback: 본문 전체에서 날짜 찾아보기
        m = re.search(r"\d{4}[.-]\d{2}[.-]\d{2}", soup.get_text(" ", strip=True))
        if m:
            s = m.group().replace(".", "-")
            return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None
    return None

def coalesce_date(list_row_date: date | None, content_html: str) -> date:
    """목록에서 날짜가 없으면 상세 페이지 날짜로"""
    if list_row_date: 
        return list_row_date
    d = parse_detail_date_from_html(content_html)
    return d or date.today()

def get_detail(driver, wait, detail_url: str, list_row_date: date | None = None):
    """상세 페이지 HTML, 썸네일, 최종 날짜 반환"""
    driver.get(detail_url)
    html = ""
    for sel in (".BD_table", ".bd_view", ".board_view", ".bd-view", ".BD_view", ".view"):
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
            elem = driver.find_element(By.CSS_SELECTOR, sel)
            html = elem.get_attribute("outerHTML") or ""
            if html.strip():
                break
        except TimeoutException:
            continue
    if not html:
        raise RuntimeError("상세 컨테이너 미발견")

    soup = BeautifulSoup(html, "html.parser")

    # <caption> 제거
    for cap in soup.find_all("caption"):
        cap.decompose()

    # 제목 행 제거
    for tr in soup.find_all("tr"):
        th = tr.find("th", class_="title")
        if th and th.has_attr("colspan") and th["colspan"] == "4":
            tr.decompose()

    # data-src → src
    for img in soup.find_all("img"):
        if img.has_attr("data-src") and not img.get("src"):
            img["src"] = img["data-src"]

    fix_relative_urls_in_soup(soup)
    content_html = str(soup)
    thumbnail = pick_thumbnail_from_content(content_html)
    final_date = coalesce_date(list_row_date, content_html)
    
    print(final_date)
    return content_html, thumbnail, final_date
