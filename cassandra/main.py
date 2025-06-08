from cassandra.cluster import Cluster
import requests
import time

# Połączenie z Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()


# Tworzenie keyspace
session.execute("""
CREATE KEYSPACE IF NOT EXISTS test_data
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

session.set_keyspace('test_data')

# Tworzenie tabel
session.execute("""
CREATE TABLE IF NOT EXISTS users (
    id int PRIMARY KEY,
    name text,
    username text,
    email text,
    street text,
    suite text,
    city text,
    zipcode text,
    geo_lat text,
    geo_lng text,
    phone text,
    website text,
    company_name text,
    company_catchPhrase text,
    company_bs text
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS posts (
    id int PRIMARY KEY,
    userId int,
    title text,
    body text
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS comments (
    id int PRIMARY KEY,
    postId int,
    name text,
    email text,
    body text
)
""")

# Pobranie danych z API
users = requests.get("https://jsonplaceholder.typicode.com/users").json()
posts = requests.get("https://jsonplaceholder.typicode.com/posts").json()
comments = requests.get("https://jsonplaceholder.typicode.com/comments").json()

# Wstawianie użytkowników (x100)
def dodaj_uzytkownikow():
    for i in range(100):
        for u in users:
            new_id = u['id'] + i * 10
            address = u.get('address', {})
            geo = address.get('geo', {})
            company = u.get('company', {})

            session.execute("""
                INSERT INTO users (id, name, username, email, street, suite, city, zipcode,
                                   geo_lat, geo_lng, phone, website, company_name, company_catchPhrase, company_bs)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                new_id, f"{u['name']} {i}", f"{u['username']}{i}", f"{i}_{u['email']}",
                address.get('street'), address.get('suite'), address.get('city'), address.get('zipcode'),
                geo.get('lat'), geo.get('lng'),
                u.get('phone'), u.get('website'),
                company.get('name'), company.get('catchPhrase'), company.get('bs')
            ))

# Wstawianie postów (x100)
def dodaj_posty():
    for i in range(100):
        for p in posts:
            new_id = p['id'] + i * 100
            new_userId = p['userId'] + i * 10
            session.execute("""
                INSERT INTO posts (id, userId, title, body)
                VALUES (%s, %s, %s, %s)
            """, (
                new_id, new_userId, f"{p['title']} [{i}]", p['body']
            ))


# Wstawianie komentarzy (x100)
def dodaj_komentarze():
    for i in range(100):
        for c in comments:
            new_id = c['id'] + i * 500
            new_postId = c['postId'] + i * 100
            session.execute("""
                INSERT INTO comments (id, postId, name, email, body)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                new_id, new_postId, f"{c['name']} [{i}]", f"{i}_{c['email']}", c['body']
            ))

print("Dane zostały pomyślnie załadowane.")

# Funkcje testujące

def zmierz_czas_operacji(nazwa, funkcja):
    start = time.time()
    funkcja()
    koniec = time.time()
    print(f"{nazwa}: {koniec - start:.2f} sekund")


def test_insert_user():
    session.execute("INSERT INTO users (id, name, username, email) VALUES (%s, %s, %s, %s)", 
                    (999, "Test User", "testuser", "test@example.com"))

def test_read_all_users():
    return list(session.execute("SELECT * FROM users"))

def test_filtered_posts():
    return list(session.execute("SELECT * FROM posts WHERE userId=1 ALLOW FILTERING"))

def test_update_user():
    session.execute("UPDATE users SET email='updated@example.com' WHERE id=999")

def test_delete_user():
    session.execute("DELETE FROM users WHERE id=999")

def test_count_posts():
    rows = session.execute("SELECT * FROM posts")
    return len(list(rows))

def measure_read_all_posts_10_proby():
    for i in range(10):
        start = time.time()
        _ = list(session.execute("SELECT * FROM posts"))
        end = time.time()
        print(f"Odczyt wszystkich postów - Próba {i+1}: {end - start} sekund")

def measure_mass_inserts_10_proby():
    for i in range(10):
        start = time.time()
        for j in range(2000 + i * 100, 2000 + (i + 1) * 100):
            session.execute("INSERT INTO posts (id, userId, title, body) VALUES (%s, %s, %s, %s)",
                            (j, 1, f"title {j}", "some body"))
        end = time.time()
        print(f"Masowe dodanie postów - Próba {i+1}: {end - start} sekund")

def measure_mass_inserts_10_proby_komentarze():
    for i in range(10):
        start = time.time()
        for j in range(2000 + i * 100, 2000 + (i + 1) * 100):
            session.execute("INSERT INTO comments (id, postId, email, body) VALUES (%s, %s, %s, %s)",
                            (j, 1, f"email {j}", "some body"))
        end = time.time()
        print(f"Masowe dodanie komentarzy - Próba {i+1}: {end - start} sekund")


def measure_mass_delete_comments_10_proby():
    for i in range(10):
        start = time.time()
        for j in range(1 + i * 10, 1 + (i + 1) * 10):
            session.execute("DELETE FROM comments WHERE id=%s", (j,))
        end = time.time()
        print(f"Masowe usunięcie komentarzy - Próba {i+1}: {end - start} sekund")

# ... (Twój dotychczasowy kod)

# Nowe testy i pomiary czasów:

def test_add_single_user_10_times():
    for i in range(10):
        start = time.time()
        session.execute("INSERT INTO users (id, name, username, email) VALUES (%s, %s, %s, %s)", 
                        (1000 + i, f"User {i}", f"user{i}", f"user{i}@example.com"))
        end = time.time()
        print(f"Dodanie użytkownika {i+1}/10: {end - start:.4f} sekund")

def test_add_single_post_10_times():
    for i in range(10):
        start = time.time()
        session.execute("INSERT INTO posts (id, userId, title, body) VALUES (%s, %s, %s, %s)",
                        (2000 + i, 1, f"title {i}", "some body"))
        end = time.time()
        print(f"Dodanie posta {i+1}/10: {end - start:.4f} sekund")

def test_add_single_comment_10_times():
    for i in range(10):
        start = time.time()
        session.execute("INSERT INTO comments (id, postId, name, email, body) VALUES (%s, %s, %s, %s, %s)",
                        (3000 + i, 1, f"Commenter {i}", f"commenter{i}@example.com", "comment body"))
        end = time.time()
        print(f"Dodanie komentarza {i+1}/10: {end - start:.4f} sekund")


def test_read_all_users_10_times():
    for i in range(10):
        start = time.time()
        _ = list(session.execute("SELECT * FROM users"))
        end = time.time()
        print(f"Odczyt wszystkich użytkowników {i+1}/10: {end - start:.4f} sekund")

def test_read_all_posts_10_times():
    for i in range(10):
        start = time.time()
        _ = list(session.execute("SELECT * FROM posts"))
        end = time.time()
        print(f"Odczyt wszystkich postów {i+1}/10: {end - start:.4f} sekund")

def test_read_all_comments_10_times():
    for i in range(10):
        start = time.time()
        _ = list(session.execute("SELECT * FROM comments"))
        end = time.time()
        print(f"Odczyt wszystkich komentarzy {i+1}/10: {end - start:.4f} sekund")


def test_filtered_reads():
    for i in range(10):
        start = time.time()
        _ = list(session.execute("SELECT * FROM posts WHERE userId=100 ALLOW FILTERING"))
        end = time.time()
        print(f"Filtrowane posty userId=100 {i+1}/10: {end - start:.4f} sekund")

    for i in range(10):
        start = time.time()
        _ = list(session.execute("SELECT * FROM comments WHERE postId=100 ALLOW FILTERING"))
        end = time.time()
        print(f"Filtrowane komentarze postId=100 {i+1}/10: {end - start:.4f} sekund")


def test_update_user_email_10_times():
    user_id = 2
    for i in range(10):
        start = time.time()
        session.execute("UPDATE users SET email=%s WHERE id=%s", (f"updated{i}@example.com", user_id))
        end = time.time()
        print(f"Update email usera id=2 {i+1}/10: {end - start:.4f} sekund")


def test_delete_user_10_times():
    base_id = 4000
    # Najpierw wstawiamy użytkowników, żeby mieć co usuwać
    for i in range(10):
        session.execute("INSERT INTO users (id, name, username, email) VALUES (%s, %s, %s, %s)",
                        (base_id + i, f"DelUser{i}", f"deluser{i}", f"del{i}@example.com"))
    for i in range(10):
        start = time.time()
        session.execute("DELETE FROM users WHERE id=%s", (base_id + i,))
        end = time.time()
        print(f"Usunięcie użytkownika {i+1}/10: {end - start:.4f} sekund")

def test_delete_post_10_times():
    base_id = 5000
    for i in range(10):
        session.execute("INSERT INTO posts (id, userId, title, body) VALUES (%s, %s, %s, %s)",
                        (base_id + i, 1, f"Del title {i}", "body"))
    for i in range(10):
        start = time.time()
        session.execute("DELETE FROM posts WHERE id=%s", (base_id + i,))
        end = time.time()
        print(f"Usunięcie posta {i+1}/10: {end - start:.4f} sekund")

def test_delete_comment_10_times():
    base_id = 6000
    for i in range(10):
        session.execute("INSERT INTO comments (id, postId, name, email, body) VALUES (%s, %s, %s, %s, %s)",
                        (base_id + i, 1, f"DelCom{i}", f"delcom{i}@example.com", "comment"))
    for i in range(10):
        start = time.time()
        session.execute("DELETE FROM comments WHERE id=%s", (base_id + i,))
        end = time.time()
        print(f"Usunięcie komentarza {i+1}/10: {end - start:.4f} sekund")


def test_count_users_10_times():
    for i in range(10):
        start = time.time()
        count = len(list(session.execute("SELECT * FROM users")))
        end = time.time()
        print(f"Zliczanie użytkowników {i+1}/10: {count} rekordów, czas: {end - start:.4f} sekund")

def test_count_posts_10_times():
    for i in range(10):
        start = time.time()
        count = len(list(session.execute("SELECT * FROM posts")))
        end = time.time()
        print(f"Zliczanie postów {i+1}/10: {count} rekordów, czas: {end - start:.4f} sekund")

def test_count_comments_10_times():
    for i in range(10):
        start = time.time()
        count = len(list(session.execute("SELECT * FROM comments")))
        end = time.time()
        print(f"Zliczanie komentarzy {i+1}/10: {count} rekordów, czas: {end - start:.4f} sekund")


if __name__ == "__main__":
    
    zmierz_czas_operacji("Czas dodawania użytkowników", dodaj_uzytkownikow)
    zmierz_czas_operacji("Czas dodawania postów", dodaj_posty)
    zmierz_czas_operacji("Czas dodawania komentarzy", dodaj_komentarze)
    test_insert_user()
    test_update_user()
    print("Filtrowane posty:", test_filtered_posts()[:2])
    print("Liczba postów:", test_count_posts())
    measure_read_all_posts_10_proby()
    measure_mass_inserts_10_proby()
    measure_mass_inserts_10_proby_komentarze()
    measure_mass_delete_comments_10_proby()
    test_delete_user()

    print("\n--- NOWE TESTY ---\n")

    test_add_single_user_10_times()
    test_add_single_post_10_times()
    test_add_single_comment_10_times()

    test_read_all_users_10_times()
    test_read_all_posts_10_times()
    test_read_all_comments_10_times()

    test_filtered_reads()

    test_update_user_email_10_times()

    test_delete_user_10_times()
    test_delete_post_10_times()
    test_delete_comment_10_times()

    test_count_users_10_times()
    test_count_posts_10_times()
    test_count_comments_10_times()
