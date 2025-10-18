from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from config import *
from parser import scan_list_page, get_detail
from sender import send_in_batches
from auth import get_access_token
import requests
from datetime import datetime, date

if __name__ == "__main__":
    # 1️⃣ 서버에서 마지막 크롤링 날짜 조회
    token = get_access_token("system", "mostem2025!")
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(f"{CRAWL_LOG_URL}/SCHOOL_NOTICE", headers=headers)
    if resp.status_code == 200:
        last_date_str = resp.json().get("lastCrawledAt")
        cutoff_date = last_date_str if last_date_str else "2025-01-01"
    else:
        cutoff_date = "2025-01-01"

    print("✅ 기준 날짜:", cutoff_date)

    # 2️⃣ 드라이버 세팅
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, TIMEOUT_SEC)

    # 3️⃣ 목록 페이지 크롤링
    driver.get(f"{START_BASE_UNIV}&currPage=1")
    items = scan_list_page(driver)
    print(f"수집된 게시글 수: {len(items)}")

    # 4️⃣ 상세 페이지 크롤링
    for it in items:
        try:
            content_html, thumbnail, final_date = get_detail(driver, wait, it["detail_url"])
            it["content_html"] = content_html
            it["thumbnail"] = thumbnail
            it["final_date"] = final_date 
        except Exception as e:
            print(f"↳ 상세 페이지 로딩 실패: {it['detail_url']} | {e}")
            it["content_html"] = ""
            it["thumbnail"] = None
            it["final_date"] = date.today() 
    # 5️⃣ DTO 구조에 맞게 변환 후 서버로 전송
    def make_payload(it):
        return {
            "title": it["title"],
            "content": it.get("content_html") or "",
            "url": it.get("detail_url") or "",
            "date": it.get("final_date").isoformat(), 
            "img": [it["thumbnail"]] if it.get("thumbnail") else []
        }

    payload_list = [make_payload(it) for it in items]
    send_in_batches(payload_list, POST_ENDPOINT_UNIV, token)

    driver.quit()
