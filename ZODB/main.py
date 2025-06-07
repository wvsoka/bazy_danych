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

# Utwórz kontenery
if not hasattr(root, 'users'):
    root.users = {}
if not hasattr(root, 'posts'):
    root.posts = {}
if not hasattr(root, 'comments'):
    root.comments = {}

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

# Testy wydajnościowe
if __name__ == "__main__":
    # Testy operacji
    zmierz_czas_operacji("Dodawanie użytkowników", dodaj_uzytkownikow)
    zmierz_czas_operacji("Dodawanie postów", dodaj_posty)
    zmierz_czas_operacji("Dodawanie komentarzy", dodaj_komentarze)
    zmierz_czas_operacji("Masowe dodanie 100 postów", masowe_dodanie_postow)
    zmierz_czas_operacji("Usunięcie 100 komentarzy", usun_komentarze)
    zmierz_czas_operacji("Odczyt wszystkich postów", odczyt_wszystkich_postow)

    # Zakończenie sesji
    connection.close()
    db.close()