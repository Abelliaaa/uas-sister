import requests

BASE_URL = "http://localhost:8080"

def test_empty_batch():
    r = requests.post(f"{BASE_URL}/publish", json={"events": []})
    assert r.status_code == 200

    data = r.json()
    assert data["received"] == 0
