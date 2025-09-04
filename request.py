#로그인 API 요청
import requests

data = {
    "username": "crawlingsys",
    "password": "cs"
}

url = "http://58.238.182.100:9000/api/member/login"

try:
    response = requests.post(url, json=data)
    print("Status Code:", response.status_code)

    if response.status_code == 200:
        print("✅ 로그인 성공")
        print("응답 데이터:", response.json())
    else:
        print("❌ 로그인 실패")
        print("응답:", response.text)

except requests.exceptions.RequestException as e:
    print("❌ 요청 에러:", e)