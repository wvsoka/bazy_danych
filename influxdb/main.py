import influxdb_client, time
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone
import requests

# --- Konfiguracja InfluxDB ---
INFLUXDB_TOKEN = "JxXksJ8Ew-pEkF0NPiKuDbfgWYO2JcuDtGbrXRN7j-FAUgHst33uJ9noyVzL1vV2U1a7YoSVLLHbzkA7rlcFbw=="
org = "projekt"
url = "http://localhost:8086"
bucket = "bucket"

client    = influxdb_client.InfluxDBClient(url=url, token=INFLUXDB_TOKEN, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)
delete_api= client.delete_api()
query_api = client.query_api()

# --- Pobranie przykładowych danych ---
users    = requests.get("https://jsonplaceholder.typicode.com/users").json()
posts    = requests.get("https://jsonplaceholder.typicode.com/posts").json()
comments = requests.get("https://jsonplaceholder.typicode.com/comments").json()

def wyczysc_bucket():
    start = "1970-01-01T00:00:00Z"
    stop  = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    delete_api.delete(start, stop, '', bucket=bucket, org=org)
    print("Bucket wyczyszczony przed testami.\n")

def run_test(nazwa, funkcja, prob=10):
    print(f"{nazwa}")
    czasy = []
    for i in range(1, prob+1):
        start = time.time()
        funkcja()
        dur = time.time() - start
        czasy.append(dur)
        print(f"Próba {i}: {dur:.4f} s")

    print()

# --- Funkcje wykonujące pojedynczą operację ---

def dodaj_jednego_uzytkownika():
    u = users[0]
    point = Point("users") \
        .tag("username", u['username']) \
        .field("id", u['id']) \
        .field("name", u['name']) \
        .field("email", u['email']) \
        .field("city", u["address"]["city"]) \
        .field("company", u["company"]["name"])
    write_api.write(bucket=bucket, org=org, record=point)

def dodaj_jednego_posta():
    p = posts[0]
    point = Point("posts") \
        .tag("userId", str(p['userId'])) \
        .field("id", p['id']) \
        .field("title", p['title']) \
        .field("body", p['body'])
    write_api.write(bucket=bucket, org=org, record=point)

def dodaj_jednego_komentarza():
    c = comments[0]
    point = Point("comments") \
        .tag("postId", str(c['postId'])) \
        .field("id", c['id']) \
        .field("name", c['name']) \
        .field("email", c['email']) \
        .field("body", c['body'])
    write_api.write(bucket=bucket, org=org, record=point)

def odczyt_wszystkich_uzytkownikow():
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "users")
    '''
    query_api.query(query, org=org)

def odczyt_wszystkich_postow():
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "posts")
    '''
    query_api.query(query, org=org)

def odczyt_wszystkich_komentarzy():
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "comments")
    '''
    query_api.query(query, org=org)

def filtruj_posty_userId_100():
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "posts" and r.userId == "100")
    '''
    query_api.query(query, org=org)

def filtruj_komentarze_postId_100():
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "comments" and r.postId == "100")
    '''
    query_api.query(query, org=org)

def modyfikuj_email_usera_2():
    # pobranie istniejącego rekordu
    q = f'''
    from(bucket: "{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "users" and r.id == 2)
      |> limit(n:1)
    '''
    tables = query_api.query(q, org=org)
    rec = None
    for t in tables:
        for r in t.records:
            rec = r
            break
    if not rec:
        return
    vals = rec.values
    point = Point("users") \
        .tag("username", vals["username"]) \
        .field("id", 2) \
        .field("name", vals["name"]) \
        .field("email", f"updated_{int(time.time())}@example.com") \
        .field("city", vals["city"]) \
        .field("company", vals["company"])
    write_api.write(bucket=bucket, org=org, record=point)

def usun_jednego_uzytkownika():
    start = "1970-01-01T00:00:00Z"
    stop  = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    pred = f'_measurement="users" AND id="{users[0]["id"]}"'
    delete_api.delete(start, stop, pred, bucket=bucket, org=org)

def usun_jednego_posta():
    start = "1970-01-01T00:00:00Z"
    stop  = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    pred = f'_measurement="posts" AND id="{posts[0]["id"]}"'
    delete_api.delete(start, stop, pred, bucket=bucket, org=org)

def usun_jednego_komentarza():
    start = "1970-01-01T00:00:00Z"
    stop  = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    pred = f'_measurement="comments" AND id="{comments[0]["id"]}"'
    delete_api.delete(start, stop, pred, bucket=bucket, org=org)

def zlicz_uzytkownikow():
    q = f'''
    from(bucket:"{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "users")
      |> distinct(column: "id")
      |> count()
    '''
    query_api.query(q, org=org)

def zlicz_posty():
    q = f'''
    from(bucket:"{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "posts")
      |> distinct(column: "id")
      |> count()
    '''
    query_api.query(q, org=org)

def zlicz_komentarze():
    q = f'''
    from(bucket:"{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "comments")
      |> distinct(column: "id")
      |> count()
    '''
    query_api.query(q, org=org)


if __name__ == "__main__":
    # wyczysc_bucket()

    # Dodawanie
    run_test("Dodanie 1 użytkownika 10 razy",       dodaj_jednego_uzytkownika)
    run_test("Dodanie 1 posta 10 razy",              dodaj_jednego_posta)
    run_test("Dodanie 1 komentarza 10 razy",         dodaj_jednego_komentarza)

    # Odczyt
    run_test("Odczyt wszystkich użytkowników 10 razy", odczyt_wszystkich_uzytkownikow)
    run_test("Odczyt wszystkich postów 10 razy",      odczyt_wszystkich_postow)
    run_test("Odczyt wszystkich komentarzy 10 razy",  odczyt_wszystkich_komentarzy)

    # Filtrowanie
    run_test("Filtrowanie postów userId=100 10 razy",    filtruj_posty_userId_100)
    run_test("Filtrowanie komentarzy postId=100 10 razy", filtruj_komentarze_postId_100)

    # Modyfikacja
    run_test("Modyfikacja email usera id=2 10 razy",   modyfikuj_email_usera_2)

    # Usuwanie
    run_test("Usunięcie 1 użytkownika 10 razy",      usun_jednego_uzytkownika)
    run_test("Usunięcie 1 posta 10 razy",            usun_jednego_posta)
    run_test("Usunięcie 1 komentarza 10 razy",       usun_jednego_komentarza)

    # Zliczanie
    run_test("Zliczanie użytkowników 10 razy",       zlicz_uzytkownikow)
    run_test("Zliczanie postów 10 razy",             zlicz_posty)
    run_test("Zliczanie komentarzy 10 razy",         zlicz_komentarze)
