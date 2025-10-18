import requests
from config import LOGIN_URL

def get_access_token(username: str, password: str) -> str:
    """로그인 후 JWT 반환"""
    resp = requests.post(LOGIN_URL, json={"username": username, "password": password})
    resp.raise_for_status()
    data = resp.json()
    return data.get("accessToken") or data.get("token")
