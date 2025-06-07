import influxdb_client, time
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone
import requests


# Konfiguracja InfluxDB
INFLUXDB_TOKEN = "yjGQGO4VdUKMVQ5s6a2A8lzkWn3ORagQJ3J1baN9M5cmeWOwrOnFb2cqF_Qf6sW2Qe4MRpg0855tS2DD2L_HMw=="
org = "projekt"
url = "http://localhost:8086"
bucket = "bucket"

client = influxdb_client.InfluxDBClient(url=url, token=INFLUXDB_TOKEN, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)
delete_api = client.delete_api()
query_api = client.query_api()

# Pobranie przykładowych danych
users = requests.get("https://jsonplaceholder.typicode.com/users").json()
posts = requests.get("https://jsonplaceholder.typicode.com/posts").json()
comments = requests.get("https://jsonplaceholder.typicode.com/comments").json()

def wyczysc_bucket():
    start = "1970-01-01T00:00:00Z"
    stop = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    delete_api.delete(start, stop, '', bucket=bucket, org=org)
    print("Bucket wyczyszczony przed testami.\n")

def zmierz_czas(nazwa, funkcja, powtorzen=1, report_each_try=False):
    czasy = []
    for i in range(powtorzen):
        start = time.time()
        funkcja()
        czas = time.time() - start
        czasy.append(czas)
        if report_each_try:
            print(f"{nazwa} - Próba {i+1}: {czas:.3f} s")
    if not report_each_try:
        print(f"{nazwa:<45} {sum(czasy)/len(czasy):.3f} s (średnio z {powtorzen} prób)")
    return czasy


def dodaj_uzytkownikow():
    for i in range(100):
        for u in users:
            new_id = u["id"] + i * 10
            point = Point("users") \
                .tag("username", f"{u['username']}{i}") \
                .field("id", new_id) \
                .field("name", f"{u['name']} {i}") \
                .field("email", f"{i}_{u['email']}") \
                .field("city", u["address"]["city"]) \
                .field("company", u["company"]["name"])
            write_api.write(bucket=bucket, org=org, record=point)

def dodaj_posty():
    for i in range(100):
        for p in posts:
            new_id = p["id"] + i * 100
            new_userId = p["userId"] + i * 10
            point = Point("posts") \
                .tag("userId", str(new_userId)) \
                .field("id", new_id) \
                .field("title", f"{p['title']} [{i}]") \
                .field("body", p["body"])
            write_api.write(bucket=bucket, org=org, record=point)

def dodaj_komentarze():
    for i in range(100):
        for c in comments:
            new_id = c["id"] + i * 500
            new_postId = c["postId"] + i * 100
            point = Point("comments") \
                .tag("postId", str(new_postId)) \
                .field("id", new_id) \
                .field("name", f"{c['name']} [{i}]") \
                .field("email", f"{i}_{c['email']}") \
                .field("body", c["body"])
            write_api.write(bucket=bucket, org=org, record=point)


# --- Testy dodawania 1 rekordu 10 razy ---

def dodaj_1_uzytkownika_10x():
    u = users[0]
    for i in range(10):
        new_id = u["id"] + i * 1000
        point = Point("users") \
            .tag("username", f"{u['username']}_test{i}") \
            .field("id", new_id) \
            .field("name", f"{u['name']} {i}") \
            .field("email", f"test{i}_{u['email']}") \
            .field("city", u["address"]["city"]) \
            .field("company", u["company"]["name"])
        write_api.write(bucket=bucket, org=org, record=point)

def dodaj_1_posta_10x():
    p = posts[0]
    for i in range(10):
        new_id = p["id"] + i * 1000
        new_userId = p["userId"] + i * 1000
        point = Point("posts") \
            .tag("userId", str(new_userId)) \
            .field("id", new_id) \
            .field("title", f"{p['title']} [{i}]") \
            .field("body", p["body"])
        write_api.write(bucket=bucket, org=org, record=point)

def dodaj_1_komentarz_10x():
    c = comments[0]
    for i in range(10):
        new_id = c["id"] + i * 1000
        new_postId = c["postId"] + i * 1000
        point = Point("comments") \
            .tag("postId", str(new_postId)) \
            .field("id", new_id) \
            .field("name", f"{c['name']} [{i}]") \
            .field("email", f"{i}_{c['email']}") \
            .field("body", c["body"])
        write_api.write(bucket=bucket, org=org, record=point)

# --- Testy odczytu 10 razy ---

def odczyt_wszystkich_uzytkownikow_10x():
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "users")
    '''
    for _ in range(10):
        tables = query_api.query(query, org=org)
        _ = [r for t in tables for r in t.records]

def odczyt_wszystkich_postow_10x():
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "posts")
    '''
    for _ in range(10):
        tables = query_api.query(query, org=org)
        _ = [r for t in tables for r in t.records]

def odczyt_wszystkich_komentarzy_10x():
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "comments")
    '''
    for _ in range(10):
        tables = query_api.query(query, org=org)
        _ = [r for t in tables for r in t.records]

# --- Filtrowanie 10 razy ---

def filtruj_posty_userId_100_10x():
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "posts" and r.userId == "100")
    '''
    for _ in range(10):
        tables = query_api.query(query, org=org)
        _ = [r for t in tables for r in t.records]

def filtruj_komentarze_postId_100_10x():
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "comments" and r.postId == "100")
    '''
    for _ in range(10):
        tables = query_api.query(query, org=org)
        _ = [r for t in tables for r in t.records]

# --- Modyfikacja email usera id=2 10 razy ---

def modyfikuj_email_usera_2_10x():
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "users" and r.id == 2)
      |> limit(n:1)
    '''
    tables = query_api.query(query, org=org)
    user_record = None
    for table in tables:
        for record in table.records:
            user_record = record
            break
    if user_record is None:
        print("Nie znaleziono usera o id=2, nie można modyfikować")
        return

    username = user_record.values.get("username")
    name = user_record.values.get("name")
    city = user_record.values.get("city")
    company = user_record.values.get("company")

    for i in range(10):
        point = Point("users") \
            .tag("username", username) \
            .field("id", 2) \
            .field("name", name) \
            .field("email", f"updated_email_{i}@example.com") \
            .field("city", city) \
            .field("company", company)
        write_api.write(bucket=bucket, org=org, record=point)

# --- Usuwanie rekordów 10 razy ---

def usun_uzytkownika_10x():
    for i in range(10):
        start = "1970-01-01T00:00:00Z"
        stop = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        predykat = f'_measurement="users" AND id="{users[0]["id"] + i * 1000}"'
        delete_api.delete(start, stop, predykat, bucket=bucket, org=org)

def usun_posta_10x():
    for i in range(10):
        start = "1970-01-01T00:00:00Z"
        stop = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        predykat = f'_measurement="posts" AND id="{posts[0]["id"] + i * 1000}"'
        delete_api.delete(start, stop, predykat, bucket=bucket, org=org)

def usun_komentarz_10x():
    for i in range(10):
        start = "1970-01-01T00:00:00Z"
        stop = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        predykat = f'_measurement="comments" AND id="{comments[0]["id"] + i * 1000}"'
        delete_api.delete(start, stop, predykat, bucket=bucket, org=org)

# --- Zliczanie 10 razy ---

def zlicz_uzytkownikow_10x():
    query = f'''
    from(bucket:"{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "users")
      |> distinct(column: "id")
      |> count()
    '''
    for _ in range(10):
        tables = query_api.query(query, org=org)
        for table in tables:
            for record in table.records:
                pass

def zlicz_posty_10x():
    query = f'''
    from(bucket:"{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "posts")
      |> distinct(column: "id")
      |> count()
    '''
    for _ in range(10):
        tables = query_api.query(query, org=org)
        for table in tables:
            for record in table.records:
                pass

def zlicz_komentarze_10x():
    query = f'''
    from(bucket:"{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "comments")
      |> distinct(column: "id")
      |> count()
    '''
    for _ in range(10):
        tables = query_api.query(query, org=org)
        for table in tables:
            for record in table.records:
                pass

# --- Testy wydajnościowe z detalicznym pomiarem dla 10 prób ---

def odczyt_wszystkich_postow_test():
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "posts")
    '''
    tables = query_api.query(query, org=org)
    _ = [r for t in tables for r in t.records]

def posty_uzytkownika_999_test():
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "posts" and r.userId == "999")
    '''
    tables = query_api.query(query, org=org)
    _ = [r for t in tables for r in t.records]

def masowe_dodanie_100_postow():
    for i in range(100):
        point = Point("posts") \
            .tag("userId", "999") \
            .field("id", 20000 + i) \
            .field("title", f"Extra {i}") \
            .field("body", "Bulk insert")
        write_api.write(bucket=bucket, org=org, record=point)

def masowe_dodanie_100_komentarzy():
    for i in range(100):
        point = Point("comments") \
            .tag("postId", "20000") \
            .field("id", 30000 + i) \
            .field("name", f"Bulk comment {i}") \
            .field("email", f"bulk{i}@example.com") \
            .field("body", "Bulk insert comment")
        write_api.write(bucket=bucket, org=org, record=point)

def masowe_usuniecie_100_komentarzy():
    start = "1970-01-01T00:00:00Z"
    stop = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    for i in range(100):
        predykat = f'_measurement="comments" AND id="{30000 + i}"'
        delete_api.delete(start, stop, predykat, bucket=bucket, org=org)

# --- Uruchomienie testów ---

if __name__ == "__main__":
    wyczysc_bucket()

    
    print("== Dodawanie wszystkich użytkowników ==")
    zmierz_czas("Dodaj użytkowników", dodaj_uzytkownikow)

    print("== Dodawanie wszystkich  postów ==")
    zmierz_czas("Dodaj posty", dodaj_posty)

    print("== Dodawanie wszystkich  komentarzy ==")
    zmierz_czas("Dodaj komentarze", dodaj_komentarze)

    print("== Dodawanie rekordów (1 user, 1 post, 1 komentarz po 10 razy) ==")
    zmierz_czas("Dodaj 1 użytkownika 10x", dodaj_1_uzytkownika_10x)
    zmierz_czas("Dodaj 1 posta 10x", dodaj_1_posta_10x)
    zmierz_czas("Dodaj 1 komentarz 10x", dodaj_1_komentarz_10x)
    print()

    print("== Odczyt danych 10 razy ==")
    zmierz_czas("Odczyt wszystkich użytkowników 10x", odczyt_wszystkich_uzytkownikow_10x)
    zmierz_czas("Odczyt wszystkich postów 10x", odczyt_wszystkich_postow_10x)
    zmierz_czas("Odczyt wszystkich komentarzy 10x", odczyt_wszystkich_komentarzy_10x)
    print()

    print("== Filtrowanie danych 10 razy ==")
    zmierz_czas("Filtrowanie postów userId=100 10x", filtruj_posty_userId_100_10x)
    zmierz_czas("Filtrowanie komentarzy postId=100 10x", filtruj_komentarze_postId_100_10x)
    print()

    print("== Modyfikacja email użytkownika id=2 (10x) ==")
    zmierz_czas("Modyfikuj email usera id=2 10x", modyfikuj_email_usera_2_10x)
    print()

    print("== Usuwanie rekordów 10 razy ==")
    zmierz_czas("Usuń użytkownika 10x", usun_uzytkownika_10x)
    zmierz_czas("Usuń posta 10x", usun_posta_10x)
    zmierz_czas("Usuń komentarz 10x", usun_komentarz_10x)
    print()

    print("== Zliczanie rekordów 10 razy ==")
    zmierz_czas("Zlicz użytkowników 10x", zlicz_uzytkownikow_10x)
    zmierz_czas("Zlicz posty 10x", zlicz_posty_10x)
    zmierz_czas("Zlicz komentarze 10x", zlicz_komentarze_10x)
    print()

    print("== Testy wydajnościowe ==")
    print("ODCZYT WSZYSTKICH POSTÓW [s]")
    zmierz_czas("Odczyt wszystkich postów", odczyt_wszystkich_postow_test, powtorzen=10, report_each_try=True)
    print()

    print("Posty użytkownika o id = 999")
    zmierz_czas("Posty usera 999", posty_uzytkownika_999_test, powtorzen=10, report_each_try=True)
    print()

    print("Masowe dodanie 100 postów")
    zmierz_czas("Masowe dodanie 100 postów", masowe_dodanie_100_postow, powtorzen=10, report_each_try=True)
    print()

    print("Masowe dodanie 100 komentarzy")
    zmierz_czas("Masowe dodanie 100 komentarzy", masowe_dodanie_100_komentarzy, powtorzen=10, report_each_try=True)
    print()

    print("Masowe usunięcie 100 komentarzy")
    zmierz_czas("Masowe usunięcie 100 komentarzy", masowe_usuniecie_100_komentarzy, powtorzen=10, report_each_try=True)
    print()
