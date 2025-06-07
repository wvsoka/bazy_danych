import requests
from requests.auth import HTTPBasicAuth

COUCHDB_URL = "http://localhost:5984"
DB_NAME = "projekt"
AUTH = HTTPBasicAuth("admin", "admin")

def delete_all_documents():
    print(f"\n🧹 Czyszczenie bazy '{DB_NAME}'...")

    # Pobierz wszystkie dokumenty (z _rev)
    res = requests.get(f"{COUCHDB_URL}/{DB_NAME}/_all_docs?include_docs=true", auth=AUTH)
    if res.status_code != 200:
        print(f"❌ Błąd pobierania dokumentów: {res.text}")
        return

    docs = res.json()["rows"]
    if not docs:
        print("✅ Baza już jest pusta.")
        return

    # Przygotuj listę do usunięcia
    to_delete = [
        {"_id": doc["id"], "_rev": doc["value"]["rev"], "_deleted": True}
        for doc in docs
    ]

    # Masowe usunięcie
    bulk_res = requests.post(
        f"{COUCHDB_URL}/{DB_NAME}/_bulk_docs",
        json={"docs": to_delete},
        auth=AUTH,
        headers={"Content-Type": "application/json"}
    )

    if bulk_res.status_code == 201:
        print(f"✅ Usunięto {len(to_delete)} dokumentów.")
    else:
        print(f"❌ Błąd przy usuwaniu: {bulk_res.text}")

delete_all_documents()
