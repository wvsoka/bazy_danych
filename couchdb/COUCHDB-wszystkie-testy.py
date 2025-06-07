import time
import json
import requests
import random
from requests.auth import HTTPBasicAuth

# Połączenie z CouchDB
COUCHDB_URL = "http://localhost:5984"
DB_NAME = "projekt"
AUTH = HTTPBasicAuth("admin", "admin")  # ← tu login i hasło
HEADERS = {"Content-Type": "application/json"}

# 0 IMPORT DANYCH DO BAZY

def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)

def upload_documents(data, prefix, doc_type):
    start = time.perf_counter()
    for item in data:
        doc = item.copy()
        doc["_id"] = f"{prefix}:{item['id']}"
        doc["type"] = doc_type
        requests.put(f"{COUCHDB_URL}/{DB_NAME}/{doc['_id']}", json=doc, auth=AUTH)
    end = time.perf_counter()
    duration = round(end - start, 4)
    print(f" Import zakończony w {duration} s ({len(data)} rekordów).")


# Wczytaj dane
users = load_json("./users_big.json")
posts = load_json("./posts_big.json")
comments = load_json("./comments_big.json")

# Import danych
print("\n Import użytkowników...")
upload_documents(users, "user", "user")

print("\n Import postów...")
upload_documents(posts, "post", "post")

print("\n Import komentarzy...")
upload_documents(comments, "comment", "comment")

print("\n Import zakończony.")


# 1 DODAWANIE NOWYCH REKORDÓW
def timed_insert(label, db, doc_template):
    print(f"\n Dodawanie {label}")
    total = 0
    for i in range(1, 11):
        doc = doc_template(i)
        url = f"{COUCHDB_URL}/{db}"
        start = time.perf_counter()
        res = requests.post(url, headers=HEADERS, data=json.dumps(doc), auth=AUTH)
        end = time.perf_counter()
        duration = round(end - start, 4)
        total += duration
        status = "✓" if res.status_code == 201 else "✗"
        print(f"  Próba {i}: {duration} s {status}")
    avg = round(total / 10, 4)
    print(f"  Średnia: {avg} s")

print("\n" + "=" * 60)
print("1. DODAWANIE NOWYCH REKORDÓW")
print("=" * 60)

# Ustawiamy nazwę jednej bazy
db_name = "projekt"

# Dodawanie użytkowników
timed_insert(
    label="użytkownika",
    db=db_name,
    doc_template=lambda i: {
        "_id": f"user:test{i}",
        "type": "user",
        "name": f"Test User {i}",
        "username": f"user{i}",
        "email": f"user{i}@example.com"
    }
)

# Dodawanie postów
timed_insert(
    label="posta",
    db=db_name,
    doc_template=lambda i: {
        "_id": f"post:test{i}",
        "type": "post",
        "userId": f"user:test{i}",
        "title": f"Test Post {i}",
        "body": f"Post body {i}"
    }
)

# Dodawanie komentarzy
timed_insert(
    label="komentarza",
    db=db_name,
    doc_template=lambda i: {
        "_id": f"comment:test{i}",
        "type": "comment",
        "postId": f"post:test{i}",
        "userId": f"user:test{i}",
        "name": f"Comment {i}",
        "email": f"comment{i}@example.com",
        "body": f"Comment body {i}"
    }
)

# 2 ODCZYT DANYCH
print("\n" + "=" * 60)
print("2. ODCZYT DANYCH")
print("=" * 60)

# a) pełne odczyty

def timed_get_all_type(label, doc_type):
    print(f"\n Odczyt wszystkich {label}")
    total = 0
    for i in range(10):
        start = time.perf_counter()
        res = requests.post(
            f"{COUCHDB_URL}/{DB_NAME}/_find",
            headers=HEADERS,
            auth=AUTH,
            json={
                "selector": {
                    "type": doc_type
                },
                "fields": ["_id"],
                "limit": 999999  # ← DODAJ TO
            }
        )
        end = time.perf_counter()
        duration = round(end - start, 4)
        count = len(res.json()["docs"]) if res.status_code == 200 else 0
        total += duration
        print(f"  Próba {i+1}: {duration} s (rekordów: {count})")
    avg = round(total / 10, 4)
    print(f"  Średnia: {avg} s")

print("\n")
print("a) PEŁNE ODCZYTY")

timed_get_all_type("użytkowników", "user")
timed_get_all_type("postów", "post")
timed_get_all_type("komentarzy", "comment")

'''

# b) filtrowane odczyty
'''
def timed_filtered_read(label, user_id):
    print(f"\n Filtrowany odczyt {label} (userId = {user_id})")
    total = 0
    for i in range(10):
        start = time.perf_counter()
        res = requests.post(
            f"{COUCHDB_URL}/{DB_NAME}/_find",
            headers=HEADERS,
            auth=AUTH,
            json={
                "selector": {
                    "type": "post",
                    "userId": user_id
                },
                "fields": ["_id", "title"],
                "limit": 999999
            }
        )
        end = time.perf_counter()
        duration = round(end - start, 4)
        count = len(res.json()["docs"]) if res.status_code == 200 else 0
        total += duration
        print(f"  Próba {i+1}: {duration} s (rekordów: {count})")
    avg = round(total / 10, 4)
    print(f"  Średnia: {avg} s")

# Testy dla userId: 999, 100, 199
print("\n")
print("b) FILTROWANE ODCZYTY")
timed_filtered_read("postów użytkownika", 999)
timed_filtered_read("postów użytkownika", 100)
timed_filtered_read("postów użytkownika", 199)


def timed_filtered_read_comments(post_id):
    print(f"\n Filtrowany odczyt komentarzy do posta (postId = {post_id})")
    total = 0
    for i in range(10):
        start = time.perf_counter()
        res = requests.post(
            f"{COUCHDB_URL}/{DB_NAME}/_find",
            headers=HEADERS,
            auth=AUTH,
            json={
                "selector": {
                    "type": "comment",
                    "postId": post_id  # liczba, nie string!
                },
                "fields": ["_id", "body"],
                "limit": 999999
            }
        )
        end = time.perf_counter()
        duration = round(end - start, 4)
        count = len(res.json()["docs"]) if res.status_code == 200 else 0
        total += duration
        print(f"  Próba {i+1}: {duration} s (rekordów: {count})")
    avg = round(total / 10, 4)
    print(f"  Średnia: {avg} s")


timed_filtered_read_comments(1000)
timed_filtered_read_comments(2000)
timed_filtered_read_comments(3000)



# 3 MODYFIKACJA ISTNIEJĄCYCH DANYCH
def timed_modify_user(user_id):
    doc_id = f"user:{user_id}"
    print(f"\n Modyfikacja użytkownika {doc_id}")
    total = 0

    for i in range(10):
        # Pobierz dokument
        get_res = requests.get(f"{COUCHDB_URL}/{DB_NAME}/{doc_id}", auth=AUTH)
        if get_res.status_code != 200:
            print(f"   Nie znaleziono {doc_id}")
            return
        doc = get_res.json()

        # Zmień email
        doc["email"] = f"updated{i+1}_{user_id}@example.com"

        # Zaktualizuj dokument
        start = time.perf_counter()
        put_res = requests.put(
            f"{COUCHDB_URL}/{DB_NAME}/{doc_id}",
            headers=HEADERS,
            auth=AUTH,
            data=json.dumps(doc)
        )
        end = time.perf_counter()

        duration = round(end - start, 4)
        total += duration

        status = "✓" if put_res.status_code == 201 else "✗"
        print(f"  Próba {i+1}: {duration} s {status} (email: {doc['email']})")

    avg = round(total / 10, 4)
    print(f"  Średnia: {avg} s")

print("\n" + "=" * 60)
print("3. MODYFIKACJA ISTNIEJĄCYCH DANYCH")
print("=" * 60)

timed_modify_user(1)
timed_modify_user(2)
timed_modify_user(3)


# 4 USUWANIE DOKUMENTÓW

def get_random_ids(doc_type, count=10):
    """Pobiera losowe ID dokumentów danego typu"""
    res = requests.post(
        f"{COUCHDB_URL}/{DB_NAME}/_find",
        headers=HEADERS,
        auth=AUTH,
        json={
            "selector": {"type": doc_type},
            "fields": ["_id"],
            "limit": 5000
        }
    )
    all_ids = [doc["_id"] for doc in res.json().get("docs", [])]
    return random.sample(all_ids, min(count, len(all_ids)))

def timed_delete(label, doc_type):
    """Usuwa losowe dokumenty danego typu i mierzy czas"""
    print(f"\n Usuwanie {label}")
    total = 0
    ids = get_random_ids(doc_type)
    for i, doc_id in enumerate(ids, start=1):
        res = requests.get(f"{COUCHDB_URL}/{DB_NAME}/{doc_id}", auth=AUTH)
        if res.status_code != 200:
            print(f"  ❌ Nie znaleziono {doc_id}")
            continue
        rev = res.json()["_rev"]
        start = time.perf_counter()
        delete_res = requests.delete(f"{COUCHDB_URL}/{DB_NAME}/{doc_id}?rev={rev}", auth=AUTH)
        end = time.perf_counter()
        duration = round(end - start, 4)
        total += duration
        status = "✓" if delete_res.status_code == 200 else "✗"
        print(f"   Próba {i}: {duration} s {status} (usunięto: {doc_id})")
    avg = round(total / len(ids), 4) if ids else 0
    print(f"  Średnia: {avg} s")

print("\n" + "=" * 60)
print("4. USUWANIE DOKUMENTÓW")
print("=" * 60)

timed_delete("użytkownika", "user")
timed_delete("posta", "post")
timed_delete("komentarza", "comment")


# 5 ZLICZANIE I AGREGACJA DANYCH

def timed_count(label, doc_type):
    print(f"\n Zliczanie {label}")
    total = 0
    for i in range(10):
        start = time.perf_counter()
        res = requests.post(
            f"{COUCHDB_URL}/{DB_NAME}/_find",
            headers=HEADERS,
            auth=AUTH,
            json={
                "selector": {"type": doc_type},
                "fields": ["_id"],
                "limit": 999999  # duży limit, żeby pobrać wszystkie
            }
        )
        end = time.perf_counter()
        count = len(res.json().get("docs", [])) if res.status_code == 200 else 0
        duration = round(end - start, 4)
        total += duration
        print(f"  Próba {i+1}: {duration} s (rekordów: {count})")
    avg = round(total / 10, 4)
    print(f"  Średnia: {avg} s")


print("\n" + "=" * 60)
print("5. ZLICZANIE I AGREGACJA DANYCH")
print("=" * 60)

timed_count("wszystkich użytkowników", "user")
timed_count("wszystkich postów", "post")
timed_count("wszystkich komentarzy", "comment")


def timed_count_filtered(label, doc_type, field, value):
    print(f"\n Zliczanie {label} gdzie {field} = {value}")
    total = 0
    for i in range(10):
        start = time.perf_counter()
        res = requests.post(
            f"{COUCHDB_URL}/{DB_NAME}/_find",
            headers=HEADERS,
            auth=AUTH,
            json={
                "selector": {
                    "type": doc_type,
                    field: value
                },
                "fields": ["_id"],
                "limit": 999999
            }
        )
        end = time.perf_counter()
        count = len(res.json().get("docs", [])) if res.status_code == 200 else 0
        duration = round(end - start, 4)
        total += duration
        print(f"  Próba {i+1}: {duration} s (rekordów: {count})")
    avg = round(total / 10, 4)
    print(f"  Średnia: {avg} s")


# Wywołania testów
timed_count_filtered("postów użytkownika", "post", "userId", 999)
timed_count_filtered("komentarzy do posta", "comment", "postId", 1234)


# 6 TESTY WYDAJNOŚCIOWE

print("\n" + "=" * 60)
print("6. TESTY WYDAJNOŚCIOWE")
print("=" * 60)

# 1. Odczyt wszystkich postów
def test_read_all_posts():
    print("\n Odczyt wszystkich postów")
    total = 0
    for i in range(10):
        start = time.perf_counter()
        res = requests.post(
            f"{COUCHDB_URL}/{DB_NAME}/_find",
            headers=HEADERS,
            auth=AUTH,
            json={
                "selector": {"type": "post"},
                "fields": ["_id"],
                "limit": 999999
            }
        )
        end = time.perf_counter()
        count = len(res.json().get("docs", []))
        duration = round(end - start, 4)
        total += duration
        print(f"  Próba {i+1}: {duration} s (rekordów: {count})")
    avg = round(total / 10, 4)
    print(f"  Średnia: {avg} s")

# 2. Posty użytkownika o id=999
def test_posts_by_user(user_id=999):
    print(f"\n Posty użytkownika o id = {user_id}")
    total = 0
    for i in range(10):
        start = time.perf_counter()
        res = requests.post(
            f"{COUCHDB_URL}/{DB_NAME}/_find",
            headers=HEADERS,
            auth=AUTH,
            json={
                "selector": {
                    "type": "post",
                    "userId": user_id
                },
                "fields": ["_id"],
                "limit": 999999
            }
        )
        end = time.perf_counter()
        count = len(res.json().get("docs", []))
        duration = round(end - start, 4)
        total += duration
        print(f"  Próba {i+1}: {duration} s (rekordów: {count})")
    avg = round(total / 10, 4)
    print(f"  Średnia: {avg} s")

# 3. Masowe dodanie 100 postów
def test_bulk_add_posts():
    print("\n Masowe dodanie 100 postów")
    total = 0
    base_id = 999900
    for i in range(10):
        docs = []
        for j in range(100):
            docs.append({
                "_id": f"post:perf{base_id + i*100 + j}",
                "type": "post",
                "userId": 999,
                "title": f"Perf Post {j}",
                "body": "Test performance body"
            })
        start = time.perf_counter()
        res = requests.post(
            f"{COUCHDB_URL}/{DB_NAME}/_bulk_docs",
            headers=HEADERS,
            auth=AUTH,
            json={"docs": docs}
        )
        end = time.perf_counter()
        duration = round(end - start, 4)
        total += duration
        print(f"  Próba {i+1}: {duration} s (id: {base_id + i*100}–{base_id + i*100 + 99})")
    avg = round(total / 10, 4)
    print(f"  Średnia: {avg} s")

# 4. Masowe dodanie 100 komentarzy
def test_bulk_add_comments():
    print("\n Masowe dodanie 100 komentarzy")
    total = 0
    base_id = 888800
    for i in range(10):
        docs = []
        for j in range(100):
            docs.append({
                "_id": f"comment:perf{base_id + i*100 + j}",
                "type": "comment",
                "postId": 1234,
                "userId": 999,
                "name": f"Comment perf {j}",
                "email": f"c{j}@example.com",
                "body": "Performance comment test"
            })
        start = time.perf_counter()
        res = requests.post(
            f"{COUCHDB_URL}/{DB_NAME}/_bulk_docs",
            headers=HEADERS,
            auth=AUTH,
            json={"docs": docs}
        )
        end = time.perf_counter()
        duration = round(end - start, 4)
        total += duration
        print(f"  Próba {i+1}: {duration} s (id: {base_id + i*100}–{base_id + i*100 + 99})")
    avg = round(total / 10, 4)
    print(f"  Średnia: {avg} s")

# 5. Masowe usunięcie tych 100 komentarzy
def test_bulk_delete_comments():
    print("\n Masowe usunięcie 100 komentarzy")
    total = 0
    base_id = 888800
    for i in range(10):
        ids = [f"comment:perf{base_id + i*100 + j}" for j in range(100)]
        docs = []
        for doc_id in ids:
            get_res = requests.get(f"{COUCHDB_URL}/{DB_NAME}/{doc_id}", auth=AUTH)
            if get_res.status_code == 200:
                doc = get_res.json()
                doc["_deleted"] = True
                docs.append(doc)
        start = time.perf_counter()
        if docs:
            del_res = requests.post(
                f"{COUCHDB_URL}/{DB_NAME}/_bulk_docs",
                headers=HEADERS,
                auth=AUTH,
                json={"docs": docs}
            )
        end = time.perf_counter()
        duration = round(end - start, 4)
        total += duration
        print(f"  Próba {i+1}: {duration} s (usunięto: {len(docs)} komentarzy)")
    avg = round(total / 10, 4)
    print(f"  Średnia: {avg} s")

# Wykonanie testów
test_read_all_posts()
test_posts_by_user()
test_bulk_add_posts()
test_bulk_add_comments()
test_bulk_delete_comments()
