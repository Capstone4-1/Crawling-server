from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from collections import defaultdict
import requests, re, datetime, json, time

# ===== config 상수 =====
from config import START_BASE_MENU, POST_ENDPOINT_MENU, CRAWL_LOG_URL

URL = START_BASE_MENU
POST_ENDPOINT = POST_ENDPOINT_MENU
CAF_STUDENT = "STUDENT"
CAF_STAFF = "STAFF"

MEAL_MAP    = {"조식": "BREAKFAST", "중식": "LUNCH", "석식": "DINNER"}
CORNER_MAP  = {"양식코너": "WESTERN", "라면코너": "RAMEN", "분식코너": "SNACK", "정식": "SET_MENU"}

# ---------- parsing helpers ----------
def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    for br in soup.find_all("br"):
        br.replace_with("\n")
    return soup.get_text("\n", strip=True)

SEC_SPLIT = re.compile(r"\n?[\[\uFF3B]\s*([^\]\uFF3D]+?)\s*[\]\uFF3D]\n?")

def split_by_sections(text: str):
    parts = SEC_SPLIT.split(text)
    sections, it = {}, iter(parts)
    _ = next(it, "")
    for sec, blob in zip(it, it):
        lines = [ln.strip() for ln in blob.splitlines() if ln.strip()]
        sections[sec] = lines
    return sections

def is_miunyeong(lines):
    return "".join(lines) == "미운영"

def parse_fc_title_to_items(inner_html: str, date_str: str):
    text = html_to_text(inner_html)
    sections = split_by_sections(text)
    items = []
    # 학생식당(코너)
    for sec, lines in sections.items():
        if sec in CORNER_MAP:
            corner = CORNER_MAP[sec]
            for name in lines:
                items.append({"cafeteriaType": CAF_STUDENT, "cornerType": corner, "name": name, "date": date_str})
    # 교직원식당(끼니)
    for sec, lines in sections.items():
        if sec in MEAL_MAP:
            meal = MEAL_MAP[sec]
            cleaned = [ln for ln in lines if ln and ln.lower() != "undefined"]
            if is_miunyeong(cleaned):
                items.append({"cafeteriaType": CAF_STAFF, "mealType": meal, "name": "(미운영)", "date": date_str})
            else:
                for name in cleaned:
                    items.append({"cafeteriaType": CAF_STAFF, "mealType": meal, "name": name, "date": date_str})
    return items

# ---------- calendar nav ----------
def wait_calendar_ready(driver):
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".fc-row.fc-week")))
    time.sleep(0.2)

def move_to_month(driver, year: int, month: int):
    wait = WebDriverWait(driver, 10)
    while True:
        hdr = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".fc-center h2"))).text
        if f"{year}년 {month}월" in hdr: return
        y, m = map(int, hdr.replace("년","").replace("월","").split())
        cur = datetime.date(y, m, 1); tgt = datetime.date(year, month, 1)
        (driver.find_element(By.CSS_SELECTOR, ".fc-next-button") if tgt > cur
         else driver.find_element(By.CSS_SELECTOR, ".fc-prev-button")).click()
        time.sleep(0.6)

def switch_view_to(driver, want: str):
    want_kw = "학생" if want == "학생" else "교직원"
    # select
    for sel in driver.find_elements(By.TAG_NAME, "select"):
        try:
            s = Select(sel)
            for opt in s.options:
                if want_kw in (opt.text or ""):
                    s.select_by_visible_text(opt.text); wait_calendar_ready(driver); return True
        except: pass
    # label/radio
    for lb in driver.find_elements(By.TAG_NAME, "label"):
        if want_kw in (lb.text or ""):
            try: lb.click(); wait_calendar_ready(driver); return True
            except: pass
    # tabs/buttons
    for el in driver.find_elements(By.CSS_SELECTOR, "a,button,li,div,span"):
        if want_kw in (el.text or "") and el.is_displayed():
            try: el.click(); wait_calendar_ready(driver); return True
            except: pass
    return False

# ---------- scrape current view ----------
def scrape_current_view_month(driver, year: int, month: int):
    by_date = defaultdict(list); ym_prefix = f"{year}-{month:02d}-"
    for week in driver.find_elements(By.CSS_SELECTOR, ".fc-row.fc-week"):
        head = week.find_elements(By.CSS_SELECTOR, ".fc-content-skeleton thead tr [data-date]")
        if not head: continue
        dates = [td.get_attribute("data-date") for td in head]
        col_buckets = [[] for _ in range(len(dates))]
        for row in week.find_elements(By.CSS_SELECTOR, ".fc-content-skeleton tbody tr"):
            tds = row.find_elements(By.CSS_SELECTOR, "td")
            for i in range(min(len(tds), len(dates))):
                col_buckets[i].append(tds[i])
        for i, date_str in enumerate(dates):
            if not date_str or not date_str.startswith(ym_prefix): continue
            for cell in col_buckets[i]:
                for t in cell.find_elements(By.CSS_SELECTOR, "span.fc-title"):
                    by_date[date_str].extend(parse_fc_title_to_items(t.get_attribute("innerHTML") or "", date_str))
    return by_date

# ---------- crawl both views -> menuWeek ----------
def crawl_month_both_views(year: int, month: int):
    opts = webdriver.ChromeOptions()
    opts.add_argument("--headless=new"); opts.add_argument("--ignore-certificate-errors")
    opts.add_argument("--disable-dev-shm-usage"); opts.add_argument("--no-sandbox"); opts.add_argument("--log-level=3")
    opts.set_capability("acceptInsecureCerts", True)

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.get(URL)

    switch_view_to(driver, "학생"); move_to_month(driver, year, month); wait_calendar_ready(driver)
    stu = scrape_current_view_month(driver, year, month)
    switch_view_to(driver, "교직원"); move_to_month(driver, year, month); wait_calendar_ready(driver)
    staff = scrape_current_view_month(driver, year, month)
    driver.quit()

    by_date = defaultdict(list)
    for d,a in stu.items(): by_date[d].extend(a)
    for d,a in staff.items(): by_date[d].extend(a)

    menu_week = []
    for d in sorted(by_date.keys()):
        student = {"WESTERN": [], "RAMEN": [], "SNACK": [], "SET_MENU": []}
        staffm  = {"BREAKFAST": [], "LUNCH": [], "DINNER": []}
        for it in by_date[d]:
            if it["cafeteriaType"] == CAF_STUDENT:
                c = it["cornerType"]; student[c].append(it["name"])
            else:
                m = it["mealType"];   staffm[m].append(it["name"])
        # 중복 제거 + 빈 키 제거
        def dedup_keep(vs):
            s=set(); out=[]
            for x in vs:
                if x not in s:
                    s.add(x); out.append(x)
            return out
        student = {k: dedup_keep(v) for k,v in student.items() if v}
        staffm  = {k: dedup_keep(v) for k,v in staffm.items()  if v}
        menu_week.append({"date": d, "studentCafeteria": student, "staffCafeteria": staffm})
    return {"menuWeek": menu_week}

# ---------- POST ----------
def post_items(items, endpoint=POST_ENDPOINT, token=None, timeout=(5,25)):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json",
    }
    body = {"items": items}
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    resp = requests.post(endpoint, headers=headers, data=data, timeout=timeout)
    return resp

def post_menu_by_batches(menu_week, batch_days=7, token = None):
    total = len(menu_week)
    i = 0
    while i < total:
        chunk = menu_week[i:i+batch_days]
        try:
            resp = post_items(chunk, token = token)
            preview = (resp.text or "")[:300]
            if 200 <= resp.status_code < 300:
                saved = inserted = updated = None
                try:
                    j = resp.json()
                    saved    = j.get("saved") or j.get("saveCount") or j.get("successCount")
                    inserted = j.get("inserted") or j.get("insertCount")
                    updated  = j.get("updated") or j.get("updateCount")
                except: pass
                if any(v is not None for v in (saved, inserted, updated)):
                    print(f"[SAVED] days {i+1}-{i+len(chunk)} / {total} → {resp.status_code} | saved:{saved} inserted:{inserted} updated:{updated}")
                else:
                    print(f"[SAVED] days {i+1}-{i+len(chunk)} / {total} → {resp.status_code} | body:{preview}")
            else:
                print(f"[FAIL ] days {i+1}-{i+len(chunk)} / {total} → {resp.status_code} | body:{preview}")
        except requests.RequestException as e:
            print(f"[ERROR] days {i+1}-{i+len(chunk)} / {total} → {e}")
        i += batch_days

# ---------- run ----------
if __name__ == "__main__":
    # 1️⃣ 서버에서 마지막 크롤링 날짜 조회
    from auth import get_access_token
    token = get_access_token("system", "mostem2025!")
    headers = {"Authorization": f"Bearer {token}"}
    print(token)
    resp = requests.get(f"{CRAWL_LOG_URL}/MEAL_MENU", headers=headers)
    if resp.status_code == 200:
        last_date_str = resp.json().get("lastCrawledAt")
        cutoff_date = last_date_str if last_date_str else "2025-01-01"
    else:
        cutoff_date = "2025-01-01"

    print("✅ 기준 날짜:", cutoff_date)
    cutoff_date = datetime.datetime.fromisoformat(cutoff_date).date()

    # 2️⃣ 월 단위 크롤링
    year, month = 2025, 10
    data = crawl_month_both_views(year, month)

    # 3️⃣ 기준 날짜 이후만 필터
    menu_week = data.get("menuWeek", [])
    menu_week = [d for d in menu_week if datetime.date.fromisoformat(d["date"]) > cutoff_date]

#     import pprint
# pp = pprint.PrettyPrinter(indent=2, width=120)
# pp.pprint(menu_week)
# print(f"총 {len(menu_week)}일치 데이터")

    if not menu_week:
        print("[WARN] menuWeek is empty after cutoff. Nothing to post.")
    else:
        # 4️⃣ 서버로 전송
        post_menu_by_batches(menu_week, batch_days=7, token = token)