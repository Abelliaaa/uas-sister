import requests
import threading
from datetime import datetime

TARGET_URL = "http://aggregator:8080/publish"
THREADS = 10

EVENT_ID = "evt-concurrent-test"

def send_event(thread_id):
    payload = {
        "events": [
            {
                "topic": "log",
                "event_id": EVENT_ID,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "source": f"thread-{thread_id}",
                "payload": {
                    "thread": thread_id
                }
            }
        ]
    }

    r = requests.post(TARGET_URL, json=payload)
    print(f"[THREAD {thread_id}] status={r.status_code}, response={r.json()}")


def main():
    threads = []

    for i in range(THREADS):
        t = threading.Thread(target=send_event, args=(i,))
        threads.append(t)

    print("[TEST] Starting concurrent publish...")
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    print("[TEST] Done")


if __name__ == "__main__":
    main()
