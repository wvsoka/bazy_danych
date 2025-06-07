import time
import requests
import sqlite3

DB_FILE = 'testdb.sqlite'


def create_tables(cur):
    cur.execute("DROP TABLE IF EXISTS comments")
    cur.execute("DROP TABLE IF EXISTS posts")
    cur.execute("DROP TABLE IF EXISTS users")

    cur.execute("""
    CREATE TABLE users (
        id        INTEGER PRIMARY KEY,
        name      TEXT,
        username  TEXT,
        email     TEXT,
        street    TEXT,
        suite     TEXT,
        city      TEXT,
        zipcode   TEXT,
        geo_lat   REAL,
        geo_lng   REAL
    )
    """)

    cur.execute("""
    CREATE TABLE posts (
        id       INTEGER PRIMARY KEY,
        user_id  INTEGER,
        title    TEXT,
        body     TEXT,
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
    )
    """)

    cur.execute("""
    CREATE TABLE comments (
        id       INTEGER PRIMARY KEY,
        post_id  INTEGER,
        name     TEXT,
        email    TEXT,
        body     TEXT,
        FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE CASCADE
    )
    """)


def fetch_and_prepare():
    users = requests.get(
        'https://jsonplaceholder.typicode.com/users'
    ).json()
    posts = requests.get(
        'https://jsonplaceholder.typicode.com/posts'
    ).json()
    comments = requests.get(
        'https://jsonplaceholder.typicode.com/comments'
    ).json()

    flat_users = []
    for u in users:
        a = u['address']
        geo = a['geo']
        flat_users.append((
            u['id'],
            u['name'],
            u['username'],
            u['email'],
            a['street'],
            a['suite'],
            a['city'],
            a['zipcode'],
            float(geo['lat']),
            float(geo['lng'])
        ))

    mapped_posts = [
        (p['id'], p['userId'], p['title'], p['body'])
        for p in posts
    ]

    mapped_comments = [
        (c['id'], c['postId'], c['name'], c['email'], c['body'])
        for c in comments
    ]

    def replicate(data):
        n = len(data)
        out = []
        for i in range(100):
            offset = i * n
            for row in data:
                r = list(row)
                r[0] = row[0] + offset
                out.append(tuple(r))
        return out

    users_rep = replicate(flat_users)
    posts_rep = replicate(mapped_posts)
    comments_rep = replicate(mapped_comments)

    return users_rep, posts_rep, comments_rep


def load_users(cur, conn, users):
    cur.executemany(
        "INSERT INTO users VALUES (?,?,?,?,?,?,?,?,?,?)",
        users
    )
    conn.commit()


def load_posts(cur, conn, posts):
    cur.executemany(
        "INSERT INTO posts VALUES (?,?,?,?)",
        posts
    )
    conn.commit()


def load_comments(cur, conn, comments):
    cur.executemany(
        "INSERT INTO comments VALUES (?,?,?,?,?)",
        comments
    )
    conn.commit()


def time_operation(func, setup, teardown, iterations=10, label=""):
    times = []
    for i in range(iterations):
        setup()
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        duration = end - start
        print(f"[{label}] Pomiar {i+1}: {duration:.6f} s")
        times.append(duration)
        teardown()
    return sum(times) / len(times)



def main():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    users, posts, comments = fetch_and_prepare()

    def teardown():
        pass

    def setup_users():
        create_tables(cur)

    def setup_posts():
        create_tables(cur)
        load_users(cur, conn, users)

    def setup_comments():
        create_tables(cur)
        load_users(cur, conn, users)
        load_posts(cur, conn, posts)

    def setup_all():
        create_tables(cur)
        load_users(cur, conn, users)
        load_posts(cur, conn, posts)
        load_comments(cur, conn, comments)

    # --- ISTNIEJĄCE TESTY ---
    avg_users = time_operation(
    lambda: load_users(cur, conn, users),
    setup_users,
    teardown,
    label="Dodawanie użytkowników (1000)"
)
    avg_posts = time_operation(lambda: load_posts(cur, conn, posts), setup_posts, teardown, label="dodawanoie postow (1000)")
    avg_comments = time_operation(lambda: load_comments(cur, conn, comments), setup_comments, teardown, label="Dodawanie komentarzy (1000)")

    max_id = max(r[0] for r in posts)
    extras = [(r[0] + max_id, r[1], r[2], r[3]) for r in posts[:100]]
    avg_mass_posts = time_operation(lambda: load_posts(cur, conn, extras), setup_posts, teardown, label="Masowe dodanie 100 postów")

    def delete_100_comments():
        cur.executemany("DELETE FROM comments WHERE id = ?", [(i,) for i in range(1, 101)])
        conn.commit()

    avg_delete = time_operation(delete_100_comments, setup_comments, teardown, label="Masowe usuniecie 100 postów")

    def read_posts():
        cur.execute("SELECT * FROM posts")
        cur.fetchall()

    avg_read = time_operation(read_posts, setup_posts, teardown, label="odczyt postow")

    def filter_posts():
        cur.execute("SELECT id, title FROM posts WHERE user_id = 1")
        cur.fetchall()

    avg_filter = time_operation(filter_posts, setup_posts, teardown, label="filtrowanie postow")

    # --- NOWE TESTY ---

    # 1. Dodawanie pojedynczych rekordów
    def add_user_10x():
        base_id = 99999
        for i in range(10):
            cur.execute("INSERT INTO users VALUES (?,?,?,?,?,?,?,?,?,?)", (
                base_id + i, f'Test{i}', f'user{i}', f'test{i}@example.com',
                '', '', '', '', 0.0, 0.0
    ))

    def add_post_10x():
        base_id = 99999
        for i in range(10):
            cur.execute("INSERT INTO posts VALUES (?,?,?,?)", (
                base_id + i, 1, f'Title {i}', f'Body {i}'
            ))

    def add_comment_10x():
        base_id = 99999
        for i in range(10):
            cur.execute("INSERT INTO comments VALUES (?,?,?,?,?)", (
                base_id + i, 1, f'Commenter {i}', f'comment{i}@example.com', f'Nice post {i}!'
            ))

    avg_add_user = time_operation(add_user_10x, setup_users, teardown, label="dodanie uzytkownika 10 x")
    avg_add_post = time_operation(add_post_10x, setup_posts, teardown, label="dodanie posta 10 x")
    avg_add_comment = time_operation(add_comment_10x, setup_comments, teardown, label="dodanie komentarza 10 x")

    # 2. Odczyt wszystkich danych
    def read_users(): cur.execute("SELECT * FROM users"); cur.fetchall()
    def read_comments(): cur.execute("SELECT * FROM comments"); cur.fetchall()

    avg_read_users = time_operation(read_users, setup_all, teardown, label="odczyt wszystkich uzytkownikow")
    avg_read_comments = time_operation(read_comments, setup_all, teardown, label="odczyt wszystkich komentarzy")

    # 3. Filtrowanie danych
    def filter_user_100(): cur.execute("SELECT * FROM posts WHERE user_id = 100"); cur.fetchall()
    def filter_comments_100(): cur.execute("SELECT * FROM comments WHERE post_id = 100"); cur.fetchall()

    avg_filter_user = time_operation(filter_user_100, setup_all, teardown , label="filtrowanie postow gdzie user id 100")
    avg_filter_comments = time_operation(filter_comments_100, setup_all, teardown, label="filtrowanie komentarzy gdzie post id 100")

    # 4. Modyfikacja danych
    def update_email_10x():
        for _ in range(10):
            cur.execute("UPDATE users SET email = 'changed@example.com' WHERE id = 2")
        conn.commit()

    avg_update_user = time_operation(update_email_10x, setup_users, teardown, label="update uzytkownika 10 x")

    # 5. Usuwanie rekordów
    def delete_user_10x():
        for _ in range(10):
            cur.execute("DELETE FROM users WHERE id = 1")
        conn.commit()

    def delete_post_10x():
        for _ in range(10):
            cur.execute("DELETE FROM posts WHERE id = 1")
        conn.commit()

    def delete_comment_10x():
        for _ in range(10):
            cur.execute("DELETE FROM comments WHERE id = 1")
        conn.commit()

    avg_del_user = time_operation(delete_user_10x, setup_all, teardown,  label="usuniecie 10 uzytkownikow")
    avg_del_post = time_operation(delete_post_10x, setup_all, teardown, label="usuniecie 10 postow")
    avg_del_comment = time_operation(delete_comment_10x, setup_all, teardown, label="usuniecie 10 koemtarzy")

    # 6. Zliczanie danych
    def count_users(): cur.execute("SELECT COUNT(*) FROM users"); cur.fetchone()
    def count_posts(): cur.execute("SELECT COUNT(*) FROM posts"); cur.fetchone()
    def count_comments(): cur.execute("SELECT COUNT(*) FROM comments"); cur.fetchone()

    avg_count_users = time_operation(count_users, setup_all, teardown, label="zliczanie uzytkownikow")
    avg_count_posts = time_operation(count_posts, setup_all, teardown,  label="zliczanie postow")
    avg_count_comments = time_operation(count_comments, setup_all, teardown, label="zliczanie koemtarzy")

    # --- WYDRUK WYNIKÓW ---
    print("\nŚrednie czasy z 10 pomiarów:")
    print(f"  Dodawanie użytkowników (1000):     {avg_users:.4f} s")
    print(f"  Dodawanie postów (10000):          {avg_posts:.4f} s")
    print(f"  Dodawanie komentarzy (50000):      {avg_comments:.4f} s")
    print(f"  Masowe dodanie 100 postów:         {avg_mass_posts:.4f} s")
    print(f"  Usunięcie 100 komentarzy:          {avg_delete:.4f} s")
    print(f"  Odczyt wszystkich postów:          {avg_read:.4f} s")
    print(f"  Filtrowanie postów (user_id=1):    {avg_filter:.4f} s")

    print("\nNowe testy:")
    print(f"  Dodanie 1 użytkownika 10x:         {avg_add_user:.4f} s")
    print(f"  Dodanie 1 posta 10x:               {avg_add_post:.4f} s")
    print(f"  Dodanie 1 komentarza 10x:          {avg_add_comment:.4f} s")
    print(f"  Odczyt wszystkich użytkowników:    {avg_read_users:.4f} s")
    print(f"  Odczyt wszystkich komentarzy:      {avg_read_comments:.4f} s")
    print(f"  Filtrowanie postów (user_id=100):  {avg_filter_user:.4f} s")
    print(f"  Filtrowanie komentarzy (post=100): {avg_filter_comments:.4f} s")
    print(f"  Modyfikacja e-maila user_id=2:     {avg_update_user:.4f} s")
    print(f"  Usunięcie użytkownika 10x:         {avg_del_user:.4f} s")
    print(f"  Usunięcie posta 10x:               {avg_del_post:.4f} s")
    print(f"  Usunięcie komentarza 10x:          {avg_del_comment:.4f} s")
    print(f"  Zliczanie użytkowników:            {avg_count_users:.4f} s")
    print(f"  Zliczanie postów:                  {avg_count_posts:.4f} s")
    print(f"  Zliczanie komentarzy:              {avg_count_comments:.4f} s")

    cur.close()
    conn.close()

    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    users, posts, comments = fetch_and_prepare()

    def teardown():
        pass

    def setup_users():
        create_tables(cur)

    def setup_posts():
        create_tables(cur)
        load_users(cur, conn, users)

    def setup_comments():
        create_tables(cur)
        load_users(cur, conn, users)
        load_posts(cur, conn, posts)

    # Średni czas dodawania użytkowników (1000 rekordów)
    avg_users = time_operation(
        lambda: load_users(cur, conn, users),
        setup_users,
        teardown, 
        label="sredni czas dodania 1000 uzytkownikow"
    )

    # Średni czas dodawania postów (10000 rekordów)
    avg_posts = time_operation(
        lambda: load_posts(cur, conn, posts),
        setup_posts,
        teardown,
        label="sredni czas dodania 10000 postow"
    )

    # Średni czas dodawania komentarzy (50000 rekordów)
    avg_comments = time_operation(
        lambda: load_comments(cur, conn, comments),
        setup_comments,
        teardown,
        label="sredni czas dodania 50000 komentarzy"
    )

    # Średni czas masowego dodania 100 dodatkowych postów
    max_id = max(r[0] for r in posts)
    extras = [
        (r[0] + max_id, r[1], r[2], r[3])
        for r in posts[:100]
    ]
    avg_mass_posts = time_operation(
        lambda: load_posts(cur, conn, extras),
        setup_posts,
        teardown,
        label="masowe dodanie 100 komentarzy sr"
    )

    # Średni czas usunięcia 100 komentarzy
    def delete_100_comments():
        cur.executemany(
            "DELETE FROM comments WHERE id = ?",
            [(i,) for i in range(1, 101)]
        )
        conn.commit()

    avg_delete = time_operation(
        delete_100_comments,
        setup_comments,
        teardown,
        label="masowe usunioecia 100 komentarzy sr"
    )

    # Średni czas odczytu wszystkich postów (SELECT * z 10000 wierszy)
    def read_posts():
        cur.execute("SELECT * FROM posts")
        cur.fetchall()

    avg_read = time_operation(
        read_posts,
        setup_posts,
        teardown,
        label="odczyt wszystkich postow"
    )

    # Średni czas filtrowania postów dla user_id = 1
    def filter_posts():
        cur.execute("SELECT id, title FROM posts WHERE user_id = 1")
        cur.fetchall()

    avg_filter = time_operation(
        filter_posts,
        setup_posts,
        teardown,
        label="filtrowanie postow user id 1"
    )

    print("Średnie czasy z 10 pomiarów:")
    print(f"  Dodawanie użytkowników:     {avg_users:.4f} s")
    print(f"  Dodawanie postów:           {avg_posts:.4f} s")
    print(f"  Dodawanie komentarzy:       {avg_comments:.4f} s")
    print(f"  Masowe dodanie 100 postów:  {avg_mass_posts:.4f} s")
    print(f"  Usunięcie 100 komentarzy:   {avg_delete:.4f} s")
    print(f"  Odczyt wszystkich postów:   {avg_read:.4f} s")
    print(f"  Filtrowanie postów user_id=1: {avg_filter:.4f} s")

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
