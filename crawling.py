import re, time, json, requests
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode
from datetime import datetime, date
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===== 설정 =====
ACCESS_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJpZCI6MTMsIm5hbWUiOiLsi5zsiqTthZwiLCJ1c2VybmFtZSI6InN5c3RlbSIsInJvbGUiOlsiUk9MRV9TWVNURU0iXSwiaWF0IjoxNzU3OTI1MDk4LCJleHAiOjE3NTc5MjY4OTh9.DOiOf_BEIPqAg0x_peWMd2aCvVdIUTe4pA6gxv1uIEk"  # 🔐 교체
CUTOFF_DATE_STR = "2025-08-25"
CUTOFF_DATE: date = datetime.strptime(CUTOFF_DATE_STR, "%Y-%m-%d").date()

LIST_BASE = "https://www.kmou.ac.kr/kmou/na/ntt/selectNttList.do?mi=2032&bbsId=10373"
POST_ENDPOINT = "http://58.238.182.100:9000/api/system/crawling-notice/univ"

TIMEOUT_SEC = 12
INCLUDE_STICKY_ONLY_FIRST_PAGE = True

# 전송 배치/재시도 (고정 배치 10)
BATCH_SIZE = 10
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 180
MAX_RETRIES_PER_BATCH = 3

# ===== 드라이버 =====
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--ignore-certificate-errors")
options.add_argument("--lang=ko-KR")
options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, TIMEOUT_SEC)

ABS_BASE = "https://www.kmou.ac.kr"

# ===== 유틸 =====
def normalize_url(u: str) -> str:
    """쿼리 정렬/중복 제거, fragment 제거 → URL 정규화"""
    try:
        p = urlparse(u)
        q = parse_qs(p.query, keep_blank_values=True)
        items = []
        for k in sorted(q.keys()):
            for v in sorted(q[k]):
                items.append((k, v))
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

def extract_nttSn_from_any(s: str):
    # URL 쿼리
    try:
        q = parse_qs(urlparse(s).query)
        if q.get("nttSn"): return q["nttSn"][0]
    except Exception:
        pass
    # onclick 패턴
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

def fix_relative_urls_in_soup(soup: BeautifulSoup):
    for tag in soup.find_all(["a", "img"]):
        attr = "href" if tag.name == "a" else "src"
        if not tag.has_attr(attr): continue
        tag[attr] = absolutize_url(tag.get(attr) or "")

def parse_detail_date_from_html(content_html: str):
    try:
        s = BeautifulSoup(content_html, "html.parser")
        lab = s.find(lambda t: t.name in ("th","dt") and "등록일" in t.get_text(strip=True))
        if lab:
            nxt = lab.find_next_sibling()
            if nxt:
                d = parse_row_date(nxt.get_text(" ", strip=True))
                if d: return d
        return parse_row_date(s.get_text(" ", strip=True))
    except Exception:
        return None

def coalesce_date(list_row_date, content_html) -> str:
    if list_row_date: return list_row_date.isoformat()
    d2 = parse_detail_date_from_html(content_html)
    return (d2 or date.today()).isoformat()

def pick_thumbnail_from_content(html: str):
    """
    본문 컨테이너 내부의 '실제' 이미지 중 첫 번째만 썸넬로.
    로고/아이콘/스프라이트/투명gif/svg 등은 휴리스틱으로 제외.
    """
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

def wait_for_list():
    """목록 테이블 등장까지 대기(여러 셀렉터 OR)"""
    wait.until(EC.any_of(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".BD_list tbody tr")),
        EC.presence_of_element_located((By.CSS_SELECTOR, ".board_list tbody tr")),
        EC.presence_of_element_located((By.CSS_SELECTOR, ".bd-list tbody tr")),
        EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
    ))

# ===== 목록 파싱 =====
def scan_list_page():
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

        row_date = None
        for td in tr.find_all("td"):
            row_date = parse_row_date(td.get_text(" ", strip=True))
            if row_date:
                break

        ntt_sn = extract_nttSn_from_any(detail_url) or extract_nttSn_from_any(a.get("onclick",""))
        items.append({
            "title": title,
            "detail_url": detail_url,
            "row_date": row_date,
            "is_sticky": sticky,
            "ntt_sn": ntt_sn
        })
    return items

# ===== 상세 파싱 =====
def get_detail(detail_url: str):
    driver.get(detail_url)
    html = ""
    for sel in (".BD_table", ".bd_view", ".board_view", ".bd-view", ".BD_view", ".view"):
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
            elem = driver.find_element(By.CSS_SELECTOR, sel)
            html = elem.get_attribute("outerHTML") or ""
            if html.strip():
                break
        except Exception:
            continue
    if not html:
        raise RuntimeError("상세 컨테이너 미발견")

    soup = BeautifulSoup(html, "html.parser")

    # <caption> 제거
    cap = soup.find("caption")
    if cap:
        cap.decompose()

    # 제목 행(<tr> 안에 <th class="title" colspan="4">) 제거
    for tr in soup.find_all("tr"):
        th = tr.find("th", class_="title")
        if th and th.has_attr("colspan") and th["colspan"] == "4":
            tr.decompose()

    # data-src → src 치환
    for img in soup.find_all("img"):
        if img.has_attr("data-src") and not img.get("src"):
            img["src"] = img["data-src"]

    # 상대경로 보정
    fix_relative_urls_in_soup(soup)

    # HTML 변환
    content_html = str(soup)

    # 본문 내부 첫 유효 이미지만 썸넬
    thumbnail = pick_thumbnail_from_content(content_html)
    return content_html, thumbnail

def fetch_detail_with_retry(url, retries=2):
    last_err = None
    for _ in range(retries + 1):
        try:
            return get_detail(url)
        except Exception as e:
            last_err = e
    raise last_err

# ===== payload 헬퍼 =====
def make_payload(it, content_html, thumb, is_sticky):
    return {
        "title": it["title"],
        "content": content_html,
        "url": normalize_url(it["detail_url"]),
        "date": coalesce_date(it["row_date"], content_html),
        "sticky": is_sticky,
        "img": [thumb] if thumb else []   # ✅ 항상 배열(없으면 빈 배열)
    }

# ===== 전송(고정 배치 10, 재시도) =====
def post_with_retries(session, url, batch, headers):
    # payload 크기(디버깅)
    try:
        payload = json.dumps(batch, ensure_ascii=False)
    except Exception as e:
        print(f"  ↳ JSON 직렬화 실패: {e}")
        return False, None

    size_kb = len(payload.encode("utf-8")) / 1024.0
    for attempt in range(1, MAX_RETRIES_PER_BATCH + 1):
        try:
            resp = session.post(
                url,
                data=payload,  # json= 대신 data= + 명시적 헤더
                headers=headers,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
            code = resp.status_code
            if 200 <= code < 300:
                return True, resp

            body_preview = (resp.text or "")[:200]
            print(f"  ↳ HTTP {code} (try {attempt}) | body: {body_preview!r}")

            if code in (408, 429, 500, 502, 503, 504):
                time.sleep(2 ** attempt)
                continue
            return False, resp

        except requests.exceptions.Timeout as e:
            print(f"  ↳ Timeout (try {attempt}) | batch≈{size_kb:.1f}KB | {e}")
            time.sleep(2 ** attempt)
        except requests.exceptions.RequestException as e:
            print(f"  ↳ RequestException (try {attempt}) | batch≈{size_kb:.1f}KB | {type(e).__name__}: {e}")
            time.sleep(2 ** attempt)

    return False, None

def send_in_batches(data, url, token):
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
        "Connection": "keep-alive",
    }

    n = len(data)
    ok = 0
    fail = 0

    for st in range(0, n, BATCH_SIZE):
        ed = min(st + BATCH_SIZE, n)
        batch = data[st:ed]
        print(f"→ 전송: {st+1}-{ed}/{n} (batch={len(batch)})")

        success, resp = post_with_retries(s, url, batch, headers)
        if success:
            ok += len(batch)
            print(f"  ✅ OK {len(batch)}건 | 누적={ok}/{n}")
        else:
            msg = (resp.text[:200] if resp is not None else "no response")
            fail += len(batch)
            print(f"  ❌ FAIL {len(batch)}건 | {msg!r}")
            # 실패해도 계속 진행. 실패 시 중단하려면 다음 줄의 주석 해제:
            # break

    print(f"[요약] 총 {n}건 중 {ok}건 성공, {fail}건 실패")

# ===== 실행 =====
total_data, seen_keys = [], set()
stats = {"sticky": 0, "normal": 0, "cutoff_stop": 0}

def make_dedupe_key(detail_url: str, ntt_sn: str | None) -> str:
    return f"sn:{ntt_sn}" if ntt_sn else f"url:{normalize_url(detail_url)}"

try:
    page, stop = 1, False
    while not stop:
        driver.get(f"{LIST_BASE}&currPage={page}")
        try:
            wait_for_list()
            items = scan_list_page()
            print(f"[디버그] 페이지 {page} 항목 수: {len(items)}")
        except TimeoutException:
            with open(f"list_{page}.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            print(f"❌ 목록 로딩 타임아웃 (list_{page}.html 저장)")
            break

        if not items:
            break

        # 공지/일반 통합 처리 (동일 규칙)
        page_cut = False
        for it in items:
            rd = it["row_date"]  # 목록에서 뽑힌 날짜 (없을 수도 있음)

            # 페이지 중지: '일반글'에서만 커트라인 미만을 만나면 중지
            # (고정 공지 때문에 스캔이 끊기는 걸 방지)
            if (not it["is_sticky"]) and rd and rd < CUTOFF_DATE:
                stats["cutoff_stop"] += 1
                page_cut = True
                break

            # 1차 필터: 목록 날짜가 커트라인 미만이면 스킵
            if rd and rd < CUTOFF_DATE:
                continue

            key = make_dedupe_key(it["detail_url"], it["ntt_sn"])
            if key in seen_keys:
                continue

            # 상세 진입 → 최종 날짜 확정(coalesce_date)
            content_html, thumb = fetch_detail_with_retry(it["detail_url"])
            final_date = datetime.fromisoformat(coalesce_date(rd, content_html)).date()
            if final_date < CUTOFF_DATE:
                continue

            payload = make_payload(it, content_html, thumb, it["is_sticky"])
            total_data.append(payload)
            seen_keys.add(key)
            stats["sticky" if it["is_sticky"] else "normal"] += 1

        if page_cut:
            stop = True
        else:
            page += 1

    print(f"[통계] 공지={stats['sticky']}, 일반={stats['normal']}, 커트라인중지={stats['cutoff_stop']}")
    print(f"[총 전송 예정] {len(total_data)}건")

    if total_data:
        send_in_batches(total_data, POST_ENDPOINT, ACCESS_TOKEN)
    else:
        print(f"ℹ️ 커트라인({CUTOFF_DATE_STR}) 이후 신규 글 없음 또는 중복")

except Exception as e:
    print("❌ 전체 오류 발생:", e)
finally:
    driver.quit()






