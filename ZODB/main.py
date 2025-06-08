import requests
import ZODB, ZODB.FileStorage
import persistent
import transaction
import time


# Tworzenie obiektów
class Osoba(persistent.Persistent):
    def __init__(self, dane):
        self.id = dane['id']
        self.name = dane['name']
        self.username = dane['username']
        self.email = dane['email']
        self.address = dane['address']
        self.phone = dane['phone']
        self.website = dane['website']
        self.company = dane['company']

class Post(persistent.Persistent):
    def __init__(self, dane):
        self.id = dane['id']
        self.userId = dane['userId']
        self.title = dane['title']
        self.body = dane['body']

class Komentarz(persistent.Persistent):
    def __init__(self, dane):
        self.id = dane['id']
        self.postId = dane['postId']
        self.name = dane['name']
        self.email = dane['email']
        self.body = dane['body']

# Pobierz dane
users = requests.get("https://jsonplaceholder.typicode.com/users").json()
posts = requests.get("https://jsonplaceholder.typicode.com/posts").json()
comments = requests.get("https://jsonplaceholder.typicode.com/comments").json()


storage = ZODB.FileStorage.FileStorage('zodb1_baza.fs')
db = ZODB.DB(storage)
connection = db.open()
root = connection.root

# inicjalizacja kontenerów
for name in ('users','posts','comments'):
    if not hasattr(root, name):
        setattr(root, name, {})

def commit():
    transaction.commit()


def dodaj_dane():
    for i in range(100):
        for u in users:
            new_id = u['id'] + i * 10
            u_copy = u.copy()
            u_copy['id'] = new_id
            u_copy['name'] += f" {i}"
            u_copy['username'] += str(i)
            u_copy['email'] = f"{i}_{u['email']}"
            root.users[str(new_id)] = Osoba(u_copy)

        for p in posts:
            new_id = p['id'] + i * 100
            p_copy = p.copy()
            p_copy['id'] = new_id
            p_copy['userId'] += i * 10
            p_copy['title'] += f" [{i}]"
            root.posts[str(new_id)] = Post(p_copy)

        for c in comments:
            new_id = c['id'] + i * 500
            c_copy = c.copy()
            c_copy['id'] = new_id
            c_copy['postId'] += i * 100
            c_copy['name'] += f" [{i}]"
            c_copy['email'] = f"{i}_{c['email']}"
            root.comments[str(new_id)] = Komentarz(c_copy)

    transaction.commit()

# Funkcja do pomiaru czasu
def zmierz_czas_operacji(nazwa, funkcja):
    start = time.time()
    funkcja()
    koniec = time.time()
    print(f"{nazwa}: {koniec - start} sekund")

# Funkcje testujące dla różnych operacji

# 1. Dodawanie użytkowników
def dodaj_uzytkownikow():
    for i in range(100):
        for u in users:
            new_id = u['id'] + i * 10
            u_copy = u.copy()
            u_copy['id'] = new_id
            u_copy['name'] += f" {i}"
            u_copy['username'] += str(i)
            u_copy['email'] = f"{i}_{u['email']}"
            root.users[str(new_id)] = Osoba(u_copy)
    transaction.commit()

# 2. Dodawanie postów
def dodaj_posty():
    for i in range(100):
        for p in posts:
            new_id = p['id'] + i * 100
            p_copy = p.copy()
            p_copy['id'] = new_id
            p_copy['userId'] += i * 10
            p_copy['title'] += f" [{i}]"
            root.posts[str(new_id)] = Post(p_copy)
    transaction.commit()

# 3. Dodawanie komentarzy
def dodaj_komentarze():
    for i in range(100):
        for c in comments:
            new_id = c['id'] + i * 500
            c_copy = c.copy()
            c_copy['id'] = new_id
            c_copy['postId'] += i * 100
            c_copy['name'] += f" [{i}]"
            c_copy['email'] = f"{i}_{c['email']}"
            root.comments[str(new_id)] = Komentarz(c_copy)
    transaction.commit()

# 4. Masowe dodanie 100 postów
def masowe_dodanie_postow():
    for i in range(2000, 2100):
        root.posts[str(i)] = Post({
            'id': i, 'userId': 1, 'title': f"title {i}", 'body': 'some body'
        })
    transaction.commit()

# 5. Usuwanie 100 komentarzy
def usun_komentarze():
    keys_to_delete = list(root.comments.keys())[:100]
    for key in keys_to_delete:
        del root.comments[key]
    transaction.commit()

# 6. Odczyt wszystkich postów
def odczyt_wszystkich_postow():
    all_posts = list(root.posts.values())
    return all_posts


# --- Wrapper pomiaru czasu 10 prób ---
def run_test(nazwa, func, prob=10):
    print(nazwa)
    czasy = []
    for i in range(1, prob+1):
        start = time.time()
        func()
        dur   = time.time() - start
        czasy.append(dur)
        print(f"Próba {i}: {dur:.5f} s")
    avg = sum(czasy)/len(czasy)
    print(f"Średni czas: {avg:.5f} s\n")

# --- Pojedyncze operacje (po 1) ---
def add_one_user():
    u = users[0].copy()
    root.users[str(u['id'])] = Osoba(u)
    commit()

def add_one_post():
    p = posts[0].copy()
    root.posts[str(p['id'])] = Post(p)
    commit()

def add_one_comment():
    c = comments[0].copy()
    root.comments[str(c['id'])] = Komentarz(c)
    commit()

def read_all_users():
    _ = list(root.users.values())

def read_all_posts():
    _ = list(root.posts.values())

def read_all_comments():
    _ = list(root.comments.values())

def filter_posts_user100():
    _ = [p for p in root.posts.values() if p.userId == 100]

def filter_comments_post100():
    _ = [c for c in root.comments.values() if c.postId == 100]

def modify_email_user2():
    # zakładamy, że klucz "2" istnieje
    if '2' in root.users:
        osoba = root.users['2']
        osoba.email = f"updated_{int(time.time())}@example.com"
        person = osoba  # oznaczamy modyfikację
        commit()

def delete_one_user():
    key = list(root.users.keys())[0]
    del root.users[key]
    commit()

def delete_one_post():
    key = list(root.posts.keys())[0]
    del root.posts[key]
    commit()

def delete_one_comment():
    key = list(root.comments.keys())[0]
    del root.comments[key]
    commit()

def count_users():
    _ = len({u.id for u in root.users.values()})

def count_posts():
    _ = len({p.id for p in root.posts.values()})

def count_comments():
    _ = len({c.id for c in root.comments.values()})

# --- Testy wydajnościowe dodatkowe ---
def read_posts_test():
    _ = list(root.posts.values())

def posts_by_user999():
    _ = [p for p in root.posts.values() if p.userId == 999]

def bulk_add_100_posts():
    base = max((int(k) for k in root.posts.keys()), default=0) + 1
    for i in range(base, base+100):
        root.posts[str(i)] = Post({'id': i,'userId':999,'title':f"t{i}",'body':"b"})
    commit()

def bulk_add_100_comments():
    base = max((int(k) for k in root.comments.keys()), default=0) + 1
    for i in range(base, base+100):
        root.comments[str(i)] = Komentarz({'id': i,'postId':20000,'name':f"n{i}",'email':"e",'body':"b"})
    commit()

def bulk_delete_100_comments():
    keys = list(root.comments.keys())[:100]
    for k in keys:
        del root.comments[k]
    commit()

def run_test(nazwa, func, prob=10):
    print(nazwa)
    czasy = []
    for i in range(1, prob+1):
        start = time.time()
        func()
        dur   = time.time() - start
        czasy.append(dur)
        print(f"Próba {i}: {dur:.5f} s")
    avg = sum(czasy)/len(czasy)
    print(f"Średni czas: {avg:.5f} s\n")

def clear_db():
    # usuń wszystkie wpisy z trzech głównych kontenerów
    root.users.clear()
    root.posts.clear()
    root.comments.clear()
    transaction.commit()

# Testy wydajnościowe
if __name__ == "__main__":
    clear_db()

    # 1) Dodawanie użytkowników
    run_test("Dodawanie użytkowników (100×100) 10 razy", dodaj_uzytkownikow)

    # 2) Dodawanie postów
    run_test("Dodawanie postów (100×100) 10 razy", dodaj_posty)

    # 3) Dodawanie komentarzy
    run_test("Dodawanie komentarzy (100×100) 10 razy", dodaj_komentarze)

    # 4) Masowe dodanie 100 postów
    run_test("Masowe dodanie 100 postów 10 razy", masowe_dodanie_postow)

    # 5) Usunięcie 100 komentarzy
    run_test("Usunięcie 100 komentarzy 10 razy", usun_komentarze)

    # 6) Odczyt wszystkich postów
    run_test("Odczyt wszystkich postów 10 razy", odczyt_wszystkich_postow)


# 1) Dodanie 1 użytkownika 10×
    run_test("Dodanie 1 użytkownika 10 razy", add_one_user)

    # 2) Dodanie 1 posta 10×
    run_test("Dodanie 1 posta 10 razy", add_one_post)

    # 3) Dodanie 1 komentarza 10×
    run_test("Dodanie 1 komentarza 10 razy", add_one_comment)

    # 4–6) Odczyty
    run_test("Odczyt wszystkich użytkowników 10 razy", read_all_users)
    run_test("Odczyt wszystkich postów 10 razy",      read_all_posts)
    run_test("Odczyt wszystkich komentarzy 10 razy",  read_all_comments)

    # 7–8) Filtrowanie
    run_test("Filtrowanie postów userId=100 10 razy",    filter_posts_user100)
    run_test("Filtrowanie komentarzy postId=100 10 razy", filter_comments_post100)

    # 9) Modyfikacja email usera id=2 10×
    run_test("Modyfikacja email usera id=2 10 razy", modify_email_user2)

    # 10–12) Usuwanie pojedynczych
    run_test("Usunięcie 1 użytkownika 10 razy",     delete_one_user)
    run_test("Usunięcie 1 posta 10 razy",           delete_one_post)
    run_test("Usunięcie 1 komentarza 10 razy",      delete_one_comment)

    # 13–15) Zliczanie unikalnych
    run_test("Zliczanie użytkowników 10 razy",      count_users)
    run_test("Zliczanie postów 10 razy",            count_posts)
    run_test("Zliczanie komentarzy 10 razy",        count_comments)

    # 16–18) Dodatkowe testy masowe
    run_test("Test odczytu wszystkich postów 10 razy",  read_posts_test)
    run_test("Posty usera 999 – filtrowanie 10 razy",   posts_by_user999)
    run_test("Masowe dodanie 100 postów 10 razy",       bulk_add_100_posts)
    run_test("Masowe dodanie 100 komentarzy 10 razy",   bulk_add_100_comments)
    run_test("Masowe usunięcie 100 komentarzy 10 razy", bulk_delete_100_comments)


    # Zakończenie sesji
    connection.close()
    db.close()