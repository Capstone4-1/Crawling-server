from datetime import date
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
# ===== 서버 관련 =====
BASE_URL = "http://localhost:8080"
LOGIN_URL = f"{BASE_URL}/api/member/login"
CRAWL_LOG_URL = f"{BASE_URL}/api/system/crawling-log"
POST_ENDPOINT_UNIV = f"{BASE_URL}/api/system/crawling-notice/univ"
POST_ENDPOINT_DEPT = f"{BASE_URL}/api/system/crawling-notice/dept"
POST_ENDPOINT_MENU = f"{BASE_URL}/api/system/crawling-menu"
ID = "system"
PW = "mostem2025!"
# ===== 크롤링 관련 =====
START_BASE_UNIV = "https://www.kmou.ac.kr/kmou/na/ntt/selectNttList.do?mi=2032&bbsId=10373"
START_BASE_DEPT = "https://www.kmou.ac.kr/ca/na/ntt/selectNttList.do?mi=777&bbsId=11666"
START_BASE_MENU = "https://www.kmou.ac.kr/coop/dv/dietView/selectDietCalendarView.do?mi=1190"

ABS_BASE = "https://www.kmou.ac.kr"



# ===== 실행 설정 =====
TIMEOUT_SEC = 12
INCLUDE_STICKY_ONLY_FIRST_PAGE = True
BATCH_SIZE = 1

# ===== 네트워크 설정 =====
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 180
MAX_RETRIES_PER_BATCH = 3




def get_driver(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--lang=ko-KR")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver