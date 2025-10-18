import requests, json
from config import CONNECT_TIMEOUT, READ_TIMEOUT, MAX_RETRIES_PER_BATCH

def post_with_retries(session, url, batch, headers):
    payload = json.dumps(batch, ensure_ascii=False)
    for attempt in range(1, MAX_RETRIES_PER_BATCH+1):
        try:
            resp = session.post(url, data=payload, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            if 200 <= resp.status_code < 300:
                return True, resp
        except requests.RequestException:
            pass
    return False, None

def send_in_batches(data, url, token):
    s = requests.Session()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
    }

    n = len(data)
    for st in range(0, n, 1):
        ed = min(st + 1, n)
        batch = data[st:ed]
        send_success, _ = post_with_retries(s, url, batch, headers)
        print(f"→ 전송 {st+1}-{ed} | 성공={send_success}")
