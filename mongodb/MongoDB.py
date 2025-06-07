import time
import requests
from pymongo import MongoClient


client = MongoClient("mongodb://127.0.0.1:27017")
db = client.testdb
users_col = db.users
posts_col = db.posts
comments_col = db.comments


def fetch_raw():
    users = requests.get("https://jsonplaceholder.typicode.com/users").json()
    posts = requests.get("https://jsonplaceholder.typicode.com/posts").json()
    comments = requests.get("https://jsonplaceholder.typicode.com/comments").json()
    return users, posts, comments


def replicate(data, key):
    n = len(data)
    out = []
    for i in range(100):
        offset = i * n
        for doc in data:
            new = doc.copy()
            new[key] = doc[key] + offset
            out.append(new)
    return out


def prepare_clean():
    users_col.drop()
    posts_col.drop()
    comments_col.drop()
    posts_col.create_index("userId")
    comments_col.create_index("postId")


def time_operation_verbose(func, setup, iterations=10, label=""):
    times = []
    for i in range(iterations):
        setup()
        start = time.perf_counter()
        func()
        duration = time.perf_counter() - start
        print(f"[{label}] Pomiar {i+1}: {duration:.6f} s")
        times.append(duration)
    return times


def main():
    # Pobranie i przygotowanie danych
    raw_users, raw_posts, raw_comments = fetch_raw()
    users = replicate(raw_users, "id")
    posts = replicate(raw_posts, "id")
    comments = replicate(raw_comments, "id")
    max_id = max(p["id"] for p in posts)
    extras = []
    for p in raw_posts[:100]:
        doc = p.copy()
        doc["id"] = p["id"] + max_id
        extras.append(doc)

    def op_insert_users():
        users_col.insert_many(users)

    def setup_users():
        prepare_clean()

    def op_insert_posts():
        posts_col.insert_many(posts)

    def setup_posts():
        prepare_clean()
        users_col.insert_many(users)

    def op_insert_comments():
        comments_col.insert_many(comments)

    def setup_comments():
        prepare_clean()
        users_col.insert_many(users)
        posts_col.insert_many(posts)

    def op_mass_posts():
        posts_col.insert_many(extras)

    def setup_mass_posts():
        prepare_clean()
        users_col.insert_many(users)
        posts_col.insert_many(posts)

    def op_delete_comments():
        comments_col.delete_many({"id": {"$lte": 100}})

    def setup_delete_comments():
        prepare_clean()
        users_col.insert_many(users)
        posts_col.insert_many(posts)
        comments_col.insert_many(comments)

    def op_read_posts():
        list(posts_col.find({}))

    def setup_read_posts():
        prepare_clean()
        users_col.insert_many(users)
        posts_col.insert_many(posts)

    def op_filter_posts():
        list(posts_col.find({"userId": 1}, {"_id": 0, "id": 1, "title": 1}))

    # Nowe testy

    # 1. Dodawanie pojedynczych rekordów
    def add_user_10x():
        base_id = 999999
        for i in range(10):
            users_col.insert_one({
                "id": base_id + i,
                "name": f"TestUser{i}",
                "username": f"user{i}",
                "email": f"test{i}@example.com",
                "address": {},
                "phone": "",
                "website": "",
                "company": {}
            })

    def setup_add_user():
        prepare_clean()

    def add_post_10x():
        base_id = 999999
        for i in range(10):
            posts_col.insert_one({
                "id": base_id + i,
                "userId": 1,
                "title": f"Title {i}",
                "body": f"Body {i}"
            })

    def setup_add_post():
        prepare_clean()
        users_col.insert_many(users)

    def add_comment_10x():
        base_id = 999999
        for i in range(10):
            comments_col.insert_one({
                "id": base_id + i,
                "postId": 1,
                "name": f"Commenter {i}",
                "email": f"comment{i}@example.com",
                "body": f"Nice post {i}!"
            })

    def setup_add_comment():
        prepare_clean()
        users_col.insert_many(users)
        posts_col.insert_many(posts)

    # 2. Odczyt danych
    def read_users():
        list(users_col.find({}))

    def read_posts_all():
        list(posts_col.find({}))

    def read_comments_all():
        list(comments_col.find({}))

    def setup_read_all():
        prepare_clean()
        users_col.insert_many(users)
        posts_col.insert_many(posts)
        comments_col.insert_many(comments)

    # 3. Filtrowanie danych
    def filter_user_100():
        list(posts_col.find({"userId": 100}))

    def filter_comments_100():
        list(comments_col.find({"postId": 100}))

    # 4. Modyfikacja danych
    def update_email_10x():
        for _ in range(10):
            users_col.update_one({"id": 2}, {"$set": {"email": "changed@example.com"}})

    def setup_update_email():
        prepare_clean()
        users_col.insert_many(users)

    # 5. Usuwanie rekordów
    def delete_user_10x():
        for _ in range(10):
            users_col.delete_one({"id": 1})

    def delete_post_10x():
        for _ in range(10):
            posts_col.delete_one({"id": 1})

    def delete_comment_10x():
        for _ in range(10):
            comments_col.delete_one({"id": 1})

    def setup_delete_all():
        prepare_clean()
        users_col.insert_many(users)
        posts_col.insert_many(posts)
        comments_col.insert_many(comments)

    # 6. Zliczanie danych
    def count_users():
        users_col.count_documents({})

    def count_posts():
        posts_col.count_documents({})

    def count_comments():
        comments_col.count_documents({})

    # --- Uruchamianie testów i wypisywanie wyników ---

    print("=== Istniejące testy ===")
    time_operation_verbose(op_insert_users, setup_users, label="Dodawanie użytkowników")
    time_operation_verbose(op_insert_posts, setup_posts, label="Dodawanie postów")
    time_operation_verbose(op_insert_comments, setup_comments, label="Dodawanie komentarzy")
    time_operation_verbose(op_mass_posts, setup_mass_posts, label="Masowe dodanie 100 postów")
    time_operation_verbose(op_delete_comments, setup_delete_comments, label="Usunięcie 100 komentarzy")
    time_operation_verbose(op_read_posts, setup_read_posts, label="Odczyt wszystkich postów")
    time_operation_verbose(op_filter_posts, setup_read_posts, label="Filtrowanie postów userId=1")

    print("\n=== Nowe testy ===")

    print("\nDodawanie 1 użytkownika 10x:")
    time_operation_verbose(add_user_10x, setup_add_user, label="Dodawanie pojedynczych użytkowników")

    print("\nDodawanie 1 posta 10x:")
    time_operation_verbose(add_post_10x, setup_add_post, label="Dodawanie pojedynczych postów")

    print("\nDodawanie 1 komentarza 10x:")
    time_operation_verbose(add_comment_10x, setup_add_comment, label="Dodawanie pojedynczych komentarzy")

    print("\nOdczyt wszystkich użytkowników (10x):")
    time_operation_verbose(read_users, setup_read_all, label="Odczyt użytkowników")

    print("\nOdczyt wszystkich postów (10x):")
    time_operation_verbose(read_posts_all, setup_read_all, label="Odczyt postów")

    print("\nOdczyt wszystkich komentarzy (10x):")
    time_operation_verbose(read_comments_all, setup_read_all, label="Odczyt komentarzy")

    print("\nFiltrowanie postów dla userId=100 (10x):")
    time_operation_verbose(filter_user_100, setup_read_all, label="Filtrowanie postów userId=100")

    print("\nFiltrowanie komentarzy dla postId=100 (10x):")
    time_operation_verbose(filter_comments_100, setup_read_all, label="Filtrowanie komentarzy postId=100")

    print("\nModyfikacja email użytkownika o id=2 (10x):")
    time_operation_verbose(update_email_10x, setup_update_email, label="Modyfikacja email")

    print("\nUsuwanie użytkownika o id=1 10x:")
    time_operation_verbose(delete_user_10x, setup_delete_all, label="Usuwanie użytkownika")

    print("\nUsuwanie posta o id=1 10x:")
    time_operation_verbose(delete_post_10x, setup_delete_all, label="Usuwanie posta")

    print("\nUsuwanie komentarza o id=1 10x:")
    time_operation_verbose(delete_comment_10x, setup_delete_all, label="Usuwanie komentarza")

    print("\nZliczanie użytkowników 10x:")
    time_operation_verbose(count_users, setup_read_all, label="Zliczanie użytkowników")

    print("\nZliczanie postów 10x:")
    time_operation_verbose(count_posts, setup_read_all, label="Zliczanie postów")

    print("\nZliczanie komentarzy 10x:")
    time_operation_verbose(count_comments, setup_read_all, label="Zliczanie komentarzy")

    max_comment_id = max(c["id"] for c in comments)
    extras_comments = []
    for c in raw_comments[:100]:
        doc = c.copy()
        doc["id"] = c["id"] + max_comment_id
        extras_comments.append(doc)

    def op_mass_comments():
        comments_col.insert_many(extras_comments)

    def setup_mass_comments():
        prepare_clean()
        users_col.insert_many(users)
        posts_col.insert_many(posts)
        comments_col.insert_many(comments)

    avg_mass_comments = time_operation_verbose(op_mass_comments, setup_mass_comments)


    client.close()


if __name__ == "__main__":
    main()
