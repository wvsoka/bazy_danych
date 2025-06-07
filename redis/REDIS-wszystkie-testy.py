import time
import json
import redis

# Połączenie z Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# IMPORT DANYCH DO BAZY

def import_data(file_path):
    with open(file_path) as f:
        data = json.load(f)
        for key, value in data:
            r.set(key, value)


# Importuj dane
print("️ Importuję użytkowników...")
start = time.perf_counter()
import_data("./redis/users_redis.json")
end = time.perf_counter()
print(f" Użytkownicy zaimportowani w {round(end - start, 4)} s.")

print("️ Importuję posty...")
start = time.perf_counter()
import_data("./redis/posts_redis.json")
end = time.perf_counter()
print(f" Posty zaimportowane w {round(end - start, 4)} s.")

print(" Importuję komentarze...")
start = time.perf_counter()
import_data("./redis/comments_redis.json")
end = time.perf_counter()
print(f" Komentarze zaimportowane w {round(end - start, 4)} s.")

print("Import zakończony.")



# 5.1	DODAWANIE NOWYCH REKORDÓW

print("\n" + "=" * 60)
print("1. DODAWANIE NOWYCH REKORDÓW")
print("=" * 60)

def timed_set(label, key_prefix, value_template):
    print(f"\n Dodawanie {label}")
    total = 0
    for i in range(1, 11):
        key = f"{key_prefix}:test{i}"
        value = json.dumps(value_template(i))
        start = time.perf_counter()
        r.set(key, value)
        end = time.perf_counter()
        duration = round(end - start, 4)
        total += duration
        print(f"  ️ Próba {i}: {duration} s (klucz: {key})")
    avg = round(total / 10, 4)
    print(f"  Średnia: {avg} s")

# Dodawanie użytkowników
timed_set(
    label="użytkownika",
    key_prefix="user",
    value_template=lambda i: {
        "name": f"Test User {i}",
        "username": f"testuser{i}",
        "email": f"user{i}@example.com"
    }
)

# Dodawanie postów
timed_set(
    label="posta",
    key_prefix="post",
    value_template=lambda i: {
        "userId": 999,
        "title": f"Test Post {i}",
        "body": f"Treść testowa posta {i}"
    }
)

# Dodawanie komentarzy
timed_set(
    label="komentarza",
    key_prefix="comment",
    value_template=lambda i: {
        "postId": 9999,
        "name": f"Commenter {i}",
        "email": f"comment{i}@example.com",
        "body": f"To jest testowy komentarz {i}."
    }
)



# 5.2	ODCZYT DANYCH
print("\n" + "=" * 60)
print("2. ODCZYT DANYCH")
print("=" * 60)

# a) pełny odczyt
print("\n")
print("a) PEŁNE ODCZYTY")

def timed_read(label, key_pattern):
    print(f"\n Odczyt wszystkich {label}")
    total = 0
    count = 0
    for i in range(10):
        start = time.perf_counter()
        keys = r.keys(key_pattern)
        values = r.mget(keys)
        end = time.perf_counter()
        duration = round(end - start, 4)
        count = len(values)
        total += duration
        print(f"   Próba {i+1}: {duration} s (rekordów: {count})")
    avg = round(total / 10, 4)
    print(f"   Średnia: {avg} s")

timed_read("użytkowników", "user:*")
timed_read("postów", "post:*")
timed_read("komentarzy", "comment:*")



# b) filtrowany odczyt
print("\n")
print("b) FILTROWANE ODCZYTY")

def timed_filtered_read(label, key_pattern, filter_field, filter_value):
    print(f"\n Filtrowany odczyt {label} gdzie {filter_field} = {filter_value}")
    total = 0
    match_count = 0
    for i in range(10):
        start = time.perf_counter()
        keys = r.keys(key_pattern)
        values = r.mget(keys)
        filtered = [v for v in values if v and json.loads(v).get(filter_field) == filter_value]
        end = time.perf_counter()
        duration = round(end - start, 4)
        match_count = len(filtered)
        total += duration
        print(f"   Próba {i+1}: {duration} s (rekordów pasujących: {match_count})")
    avg = round(total / 10, 4)
    print(f"   Średnia: {avg} s")

# Test: posty użytkownika userId = 999 (powtórz dla 3 userId)
timed_filtered_read("postów użytkownika", "post:*", "userId", 999)
timed_filtered_read("postów użytkownika", "post:*", "userId", 100)
timed_filtered_read("postów użytkownika", "post:*", "userId", 199)

# Test: komentarze do posta postId = 1000, 2000, 3000
timed_filtered_read("komentarzy do posta", "comment:*", "postId", 1000)
timed_filtered_read("komentarzy do posta", "comment:*", "postId", 2000)
timed_filtered_read("komentarzy do posta", "comment:*", "postId", 3000)




# 5.3	MODYFIKACJA ISTNIEJĄCYCH DANYCH
print("\n" + "=" * 60)
print("3. MODYFIKACJA ISTNIEJĄCYCH DANYCH")
print("=" * 60)

def timed_modify_user(user_key, i):
    print(f"\n Modyfikacja użytkownika {user_key}")
    total = 0
    for j in range(10):
        # GET
        start = time.perf_counter()
        raw = r.get(user_key)
        if not raw:
            print(f" Nie znaleziono klucza {user_key}")
            return
        user = json.loads(raw)
        
        # Modyfikacja
        user["email"] = f"updated{j+1}_{i}@example.com"

        # SET
        r.set(user_key, json.dumps(user))
        end = time.perf_counter()
        duration = round(end - start, 4)
        total += duration
        print(f"  ️ Próba {j+1}: {duration} s (nowy email: {user['email']})")
    avg = round(total / 10, 4)
    print(f"   Średnia: {avg} s")

# Testy dla trzech użytkowników
timed_modify_user("user:1", 1)
timed_modify_user("user:2", 2)
timed_modify_user("user:3", 3)




# 5.4	USUWANIE DOKUMENTÓW
print("\n" + "=" * 60)
print("4. USUWANIE DOKUMENTÓW")
print("=" * 60)

def timed_delete(label, key_prefix):
    print(f"\n Usuwanie {label}")
    total = 0
    for i in range(1, 11):
        key = f"{key_prefix}:test{i}"
        if not r.exists(key):
            print(f"  ️ Klucz {key} nie istnieje — pomijam")
            continue
        start = time.perf_counter()
        r.delete(key)
        end = time.perf_counter()
        duration = round(end - start, 4)
        total += duration
        print(f"   Próba {i}: {duration} s (usunięto: {key})")
    avg = round(total / 10, 4)
    print(f"  Średnia: {avg} s")

timed_delete("użytkownika", "user")
timed_delete("posta", "post")
timed_delete("komentarza", "comment")




# 5.5	ZLICZANIE I AGREGACJA DANYCH
print("\n" + "=" * 60)
print("5. ZLICZANIE I AGREGACJA DANYCH")
print("=" * 60)

def count_keys(label, pattern):
    print(f"\n Zliczanie wszystkich {label}")
    total = 0
    count = 0
    for i in range(10):
        start = time.perf_counter()
        keys = r.keys(pattern)
        count = len(keys)
        end = time.perf_counter()
        duration = round(end - start, 4)
        total += duration
        print(f"  ️ Próba {i+1}: {duration} s (rekordów: {count})")
    avg = round(total / 10, 4)
    print(f"   Średnia: {avg} s")

def count_filtered(label, key_pattern, field, value):
    print(f"\n Zliczanie {label} gdzie {field} = {value}")
    total = 0
    count = 0
    for i in range(10):
        start = time.perf_counter()
        keys = r.keys(key_pattern)
        values = r.mget(keys)
        count = sum(1 for v in values if v and json.loads(v).get(field) == value)
        end = time.perf_counter()
        duration = round(end - start, 4)
        total += duration
        print(f"  ️ Próba {i+1}: {duration} s (dopasowań: {count})")
    avg = round(total / 10, 4)
    print(f"   Średnia: {avg} s")

# Zliczanie wszystkich użytkowników, postów, komentarzy
count_keys("użytkowników", "user:*")
count_keys("postów", "post:*")
count_keys("komentarzy", "comment:*")

# Zliczanie postów userId = 999, 500, 1000
count_filtered("postów użytkownika", "post:*", "userId", 999)
#count_filtered("postów użytkownika", "post:*", "userId", 500)
#count_filtered("postów użytkownika", "post:*", "userId", 1000)

# Zliczanie komentarzy do postId = 1234, 4567, 9999
count_filtered("komentarzy do posta", "comment:*", "postId", 1234)
#count_filtered("komentarzy do posta", "comment:*", "postId", 4567)
#count_filtered("komentarzy do posta", "comment:*", "postId", 9999)




# 5.6	TESTY WYDAJNOŚCIOWE
print("\n" + "=" * 60)
print("6. TESTY WYDAJNOŚCIOWE")
print("=" * 60)

def read_all_posts():
    total = 0
    count = 0
    print("\n Odczyt wszystkich postów")
    for i in range(10):
        start = time.perf_counter()
        keys = r.keys("post:*")
        values = r.mget(keys)
        end = time.perf_counter()
        count = len(values)
        duration = round(end - start, 4)
        total += duration
        print(f"  ️ Próba {i+1}: {duration} s (rekordów: {count})")
    avg = round(total / 10, 4)
    print(f"   Średnia: {avg} s")

def filtered_read_user_posts(user_id):
    total = 0
    count = 0
    print(f"\n Posty użytkownika o id = {user_id}")
    for i in range(10):
        start = time.perf_counter()
        keys = r.keys("post:*")
        values = r.mget(keys)
        filtered = [v for v in values if v and json.loads(v).get("userId") == user_id]
        end = time.perf_counter()
        count = len(filtered)
        duration = round(end - start, 4)
        total += duration
        print(f"   Próba {i+1}: {duration} s (dopasowań: {count})")
    avg = round(total / 10, 4)
    print(f"  Średnia: {avg} s")

def mass_insert_posts():
    print("\n Masowe dodanie 100 postów")
    total = 0
    for i in range(10):
        start_id = 200000 + (i * 100)
        start = time.perf_counter()
        for j in range(100):
            pid = start_id + j
            key = f"post:perf{pid}"
            value = {
                "userId": 999,
                "title": f"Post {pid}",
                "body": "Wydajnościowy test"
            }
            r.set(key, json.dumps(value))
        end = time.perf_counter()
        duration = round(end - start, 4)
        total += duration
        print(f"  ️ Próba {i+1}: {duration} s (posty: {start_id}–{start_id+99})")
    avg = round(total / 10, 4)
    print(f"   Średnia: {avg} s")

#dodanie komntarzy do usuniecia zaraz
def mass_insert_comments():
    print("\n Masowe dodanie 100 komentarzy")
    total = 0
    for i in range(10):
        start_id = 80000 + (i * 100)
        start = time.perf_counter()
        for j in range(100):
            cid = start_id + j
            key = f"comment:perf{cid}"
            value = {
                "postId": 9999,
                "name": f"Commenter {cid}",
                "email": f"perf_comment{cid}@example.com",
                "body": "Komentarz do testów wydajności"
            }
            r.set(key, json.dumps(value))
        end = time.perf_counter()
        duration = round(end - start, 4)
        total += duration
        print(f"  Próba {i+1}: {duration} s (komentarze: {start_id}–{start_id+99})")
    avg = round(total / 10, 4)
    print(f"   Średnia: {avg} s")


def mass_delete_comments():
    print("\n Masowe usunięcie 100 komentarzy")
    total = 0
    for i in range(10):
        start_id = 80000 + (i * 100)
        start = time.perf_counter()
        for j in range(100):
            cid = start_id + j
            key = f"comment:perf{cid}"
            r.delete(key)
        end = time.perf_counter()
        duration = round(end - start, 4)
        total += duration
        print(f"  ️ Próba {i+1}: {duration} s (komentarze: {start_id}–{start_id+99})")
    avg = round(total / 10, 4)
    print(f"   Średnia: {avg} s")

# Wykonaj wszystkie testy
read_all_posts()
filtered_read_user_posts(999)
mass_insert_posts()
mass_insert_comments()
mass_delete_comments()


