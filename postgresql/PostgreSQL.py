import time
import requests
import psycopg2
from psycopg2.extras import execute_values

ADMIN_DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'haslo',
    'host': '127.0.0.1',
    'port': 5432,
}

TEST_DB_PARAMS = {
    'dbname': 'testdb',
    'user': 'postgres',
    'password': 'haslo',
    'host': '127.0.0.1',
    'port': 5432,
}

URL_USERS = 'https://jsonplaceholder.typicode.com/users'
URL_POSTS = 'https://jsonplaceholder.typicode.com/posts'
URL_COMMENTS = 'https://jsonplaceholder.typicode.com/comments'


def ensure_testdb_exists():
    conn = psycopg2.connect(**ADMIN_DB_PARAMS)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = 'testdb'")
            if cur.fetchone() is None:
                cur.execute("CREATE DATABASE testdb")
    finally:
        conn.close()


def create_tables(cur):
    cur.execute("DROP TABLE IF EXISTS comments")
    cur.execute("DROP TABLE IF EXISTS posts")
    cur.execute("DROP TABLE IF EXISTS users")

    cur.execute(
        "CREATE TABLE users ("
        "id INT PRIMARY KEY, "
        "name TEXT, "
        "username TEXT, "
        "email TEXT, "
        "street TEXT, "
        "suite TEXT, "
        "city TEXT, "
        "zipcode TEXT, "
        "geo_lat NUMERIC, "
        "geo_lng NUMERIC"
        ")"
    )

    cur.execute(
        "CREATE TABLE posts ("
        "id INT PRIMARY KEY, "
        "user_id INT REFERENCES users(id), "
        "title TEXT, "
        "body TEXT"
        ")"
    )

    cur.execute(
        "CREATE TABLE comments ("
        "id INT PRIMARY KEY, "
        "post_id INT REFERENCES posts(id), "
        "name TEXT, "
        "email TEXT, "
        "body TEXT"
        ")"
    )


def fetch_and_prepare():
    with requests.Session() as sess:
        users = sess.get(URL_USERS).json()
        posts = sess.get(URL_POSTS).json()
        comments = sess.get(URL_COMMENTS).json()

    # Flattening danych użytkowników
    flat_users = []
    for u in users:
        a = u['address']
        geo = a['geo']
        flat_users.append({
            'id': u['id'],
            'name': u['name'],
            'username': u['username'],
            'email': u['email'],
            'street': a['street'],
            'suite': a['suite'],
            'city': a['city'],
            'zipcode': a['zipcode'],
            'geo_lat': float(geo['lat']),
            'geo_lng': float(geo['lng']),
        })

    # Mapowanie postów
    mapped_posts = []
    for p in posts:
        mapped_posts.append({
            'id': p['id'],
            'user_id': p['userId'],
            'title': p['title'],
            'body': p['body'],
        })

    # Mapowanie komentarzy
    mapped_comments = []
    for c in comments:
        mapped_comments.append({
            'id': c['id'],
            'post_id': c['postId'],
            'name': c['name'],
            'email': c['email'],
            'body': c['body'],
        })

    def replicate(lst, key):
        n = len(lst)
        out = []
        for i in range(100):
            offset = i * n
            for r in lst:
                cpy = r.copy()
                cpy[key] = r[key] + offset
                out.append(cpy)
        return out

    users_rep = replicate(flat_users, 'id')
    posts_rep = replicate(mapped_posts, 'id')
    comments_rep = replicate(mapped_comments, 'id')

    return users_rep, posts_rep, comments_rep


def load_users(cur, conn, users):
    execute_values(
        cur,
        "INSERT INTO users "
        "(id, name, username, email, street, suite, city, zipcode, geo_lat, geo_lng) "
        "VALUES %s",
        users,
        template="(%(id)s, %(name)s, %(username)s, %(email)s, "
                 "%(street)s, %(suite)s, %(city)s, %(zipcode)s, "
                 "%(geo_lat)s, %(geo_lng)s)",
    )
    conn.commit()


def load_posts(cur, conn, posts):
    execute_values(
        cur,
        "INSERT INTO posts (id, user_id, title, body) VALUES %s",
        posts,
        template="(%(id)s, %(user_id)s, %(title)s, %(body)s)",
    )
    conn.commit()


def load_comments(cur, conn, comments):
    execute_values(
        cur,
        "INSERT INTO comments (id, post_id, name, email, body) VALUES %s",
        comments,
        template="(%(id)s, %(post_id)s, %(name)s, %(email)s, %(body)s)",
    )
    conn.commit()


def time_operation(func, setup, teardown, iterations=10):
    durations = []
    for _ in range(iterations):
        setup()
        start = time.perf_counter()
        func()
        durations.append(time.perf_counter() - start)
        teardown()
    return sum(durations) / len(durations)


def measure_with_details(func, setup, teardown, label, iterations=10, extra_info_func=None):
    durations = []
    print(f"  {label}")
    for i in range(1, iterations + 1):
        setup()
        start = time.perf_counter()
        result = func()
        end = time.perf_counter()
        duration = end - start
        durations.append(duration)
        extra_info = f" ({extra_info_func(result)})" if extra_info_func else ""
        print(f"  Próba {i}: {duration:.4f} s{extra_info}")
        teardown()
    avg = sum(durations) / iterations
    print(f"  Średnia: {avg:.4f} s\n")



def main():
    ensure_testdb_exists()
    users, posts, comments = fetch_and_prepare()

    with psycopg2.connect(**TEST_DB_PARAMS) as conn, conn.cursor() as cur:
        def setup_schema():
            create_tables(cur)

        def teardown_all():
            cur.execute("TRUNCATE comments, posts, users")
            conn.commit()

        # lista dodatkowych 100 postów do masowego wstawiania
        max_id = max(p['id'] for p in posts)
        extras = []
        for p in posts[:100]:
            new_post = p.copy()
            new_post['id'] = p['id'] + max_id
            extras.append(new_post)

        # Mierzone wstawianie użytkowników
        avg_users = time_operation(
            lambda: load_users(cur, conn, users),
            setup_schema,
            teardown_all,
        )

        # Mierzone wstawianie postów
        def setup_posts():
            create_tables(cur)
            load_users(cur, conn, users)

        avg_posts = time_operation(
            lambda: load_posts(cur, conn, posts),
            setup_posts,
            teardown_all,
        )

        # Mierzone wstawianie komentarzy
        def setup_comments():
            create_tables(cur)
            load_users(cur, conn, users)
            load_posts(cur, conn, posts)

        avg_comments = time_operation(
            lambda: load_comments(cur, conn, comments),
            setup_comments,
            teardown_all,
        )

        # Mierzone masowe wstawianie 100 dodatkowych postów
        def setup_mass():
            setup_comments()

        avg_mass = time_operation(
            lambda: load_posts(cur, conn, extras),
            setup_mass,
            teardown_all,
        )

        # Mierzone usunięcie 100 komentarzy
        def setup_delete():
            setup_comments()

        def delete_100_comments():
            cur.execute("DELETE FROM comments WHERE id <= 100")
            conn.commit()

        avg_delete = time_operation(
            delete_100_comments,
            setup_delete,
            teardown_all,
        )

        # Mierzone odczytanie wszystkich postów
        def setup_read():
            setup_posts()

        def read_all_posts():
            cur.execute("SELECT * FROM posts")
            _ = cur.fetchall()

        avg_read = time_operation(
            read_all_posts,
            setup_read,
            teardown_all,
        )

        # Mierzone filtrowanie postów dla user_id = 1
        def filter_posts_user_1():
            cur.execute(
                "SELECT id, title FROM posts WHERE user_id = 1"
            )
            _ = cur.fetchall()

        avg_filter = time_operation(
            filter_posts_user_1,
            setup_read,
            teardown_all,
        )

        # Odczyt wszystkich postów
        measure_with_details(
            func=lambda: cur.execute("SELECT * FROM posts") or cur.fetchall(),
            setup=setup_read,
            teardown=teardown_all,
            label="Odczyt wszystkich postów",
            extra_info_func=lambda res: f"rekordów: {len(res)}"
        )

        # Filtrowanie postów użytkownika 999
        def filter_user_999():
            cur.execute("SELECT * FROM posts WHERE user_id = 999")
            return cur.fetchall()

        measure_with_details(
            func=filter_user_999,
            setup=setup_read,
            teardown=teardown_all,
            label="Posty użytkownika o id = 999",
            extra_info_func=lambda res: f"rekordów: {len(res)}"
        )

        # Masowe dodanie 100 postów
        def mass_insert_post_factory(start_id):
            def inner():
                new_posts = []
                for i, p in enumerate(posts[:100]):
                    post = p.copy()
                    post['id'] = start_id + i
                    new_posts.append(post)
                load_posts(cur, conn, new_posts)
                return new_posts
            return inner

        measure_with_details(
            func=mass_insert_post_factory(999900),
            setup=setup_mass,
            teardown=teardown_all,
            label="Masowe dodanie 100 postów",
            extra_info_func=lambda res: f"id: {res[0]['id']}–{res[-1]['id']}"
        )

        # Masowe dodanie 100 komentarzy
        def mass_insert_comments_factory(start_id):
            def inner():
                new_comments = []
                for i, c in enumerate(comments[:100]):
                    comment = c.copy()
                    comment['id'] = start_id + i
                    comment['post_id'] = comments[i]['post_id']
                    new_comments.append(comment)
                load_comments(cur, conn, new_comments)
                return new_comments
            return inner

        measure_with_details(
            func=mass_insert_comments_factory(888800),
            setup=setup_comments,
            teardown=teardown_all,
            label="Masowe dodanie 100 komentarzy",
            extra_info_func=lambda res: f"id: {res[0]['id']}–{res[-1]['id']}"
        )

        # Masowe usunięcie 100 komentarzy
        def delete_100_comments_details():
            cur.execute("DELETE FROM comments WHERE id BETWEEN 888800 AND 888899")
            deleted = cur.rowcount
            conn.commit()
            return deleted

        measure_with_details(
            func=delete_100_comments_details,
            setup=setup_comments,
            teardown=teardown_all,
            label="Masowe usunięcie 100 komentarzy",
            extra_info_func=lambda count: f"usunięto: {count} komentarzy"
        )

                # Dodawanie pojedynczych rekordów 10 razy
        def insert_user_10x():
            for _ in range(10):
                cur.execute(
                    "INSERT INTO users (id, name, username, email, street, suite, city, zipcode, geo_lat, geo_lng) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (9999, 'Test User', 'testuser', 'test@example.com',
                     'Street', 'Suite', 'City', '00000', 0.0, 0.0)
                )
                conn.commit()
                cur.execute("DELETE FROM users WHERE id = 9999")
                conn.commit()

        measure_with_details(
            insert_user_10x,
            setup=setup_schema,
            teardown=teardown_all,
            label="Dodanie użytkownika 10 razy"
        )

        def insert_post_10x():
            load_users(cur, conn, users[:1])
            for _ in range(10):
                cur.execute(
                    "INSERT INTO posts (id, user_id, title, body) VALUES (%s, %s, %s, %s)",
                    (9999, users[0]['id'], 'Test Title', 'Test Body')
                )
                conn.commit()
                cur.execute("DELETE FROM posts WHERE id = 9999")
                conn.commit()

        measure_with_details(
            insert_post_10x,
            setup=setup_schema,
            teardown=teardown_all,
            label="Dodanie posta 10 razy"
        )

        def insert_comment_10x():
            load_users(cur, conn, users[:1])
            load_posts(cur, conn, posts[:1])
            for _ in range(10):
                cur.execute(
                    "INSERT INTO comments (id, post_id, name, email, body) VALUES (%s, %s, %s, %s, %s)",
                    (9999, posts[0]['id'], 'Test Comment', 'comment@example.com', 'Body')
                )
                conn.commit()
                cur.execute("DELETE FROM comments WHERE id = 9999")
                conn.commit()

        measure_with_details(
            insert_comment_10x,
            setup=setup_schema,
            teardown=teardown_all,
            label="Dodanie komentarza 10 razy"
        )

        # Odczyt danych 10x
        def read_users_10x():
            load_users(cur, conn, users)
            for _ in range(10):
                cur.execute("SELECT * FROM users")
                cur.fetchall()

        measure_with_details(
            read_users_10x,
            setup=setup_schema,
            teardown=teardown_all,
            label="Odczyt użytkowników 10 razy"
        )

        def read_posts_10x():
            setup_read()
            for _ in range(10):
                cur.execute("SELECT * FROM posts")
                cur.fetchall()

        measure_with_details(
            read_posts_10x,
            setup=setup_read,
            teardown=teardown_all,
            label="Odczyt postów 10 razy"
        )

        def read_comments_10x():
            setup_comments()
            for _ in range(10):
                cur.execute("SELECT * FROM comments")
                cur.fetchall()

        measure_with_details(
            read_comments_10x,
            setup=setup_comments,
            teardown=teardown_all,
            label="Odczyt komentarzy 10 razy"
        )

        # Filtrowanie danych
        def filter_posts_user_100():
            load_users(cur, conn, users)
            load_posts(cur, conn, posts)
            for _ in range(10):
                cur.execute("SELECT * FROM posts WHERE user_id = 100")
                cur.fetchall()

        measure_with_details(
            filter_posts_user_100,
            setup=setup_schema,
            teardown=teardown_all,
            label="Filtrowanie postów user_id = 100"
        )

        def filter_comments_post_100():
            setup_comments()
            for _ in range(10):
                cur.execute("SELECT * FROM comments WHERE post_id = 100")
                cur.fetchall()

        measure_with_details(
            filter_comments_post_100,
            setup=setup_comments,
            teardown=teardown_all,
            label="Filtrowanie komentarzy dla post_id = 100"
        )

        # Modyfikacja danych
        def update_user_email_10x():
            load_users(cur, conn, users)
            for i in range(10):
                cur.execute("UPDATE users SET email = %s WHERE id = 2", (f"user2_test{i}@example.com",))
                conn.commit()

        measure_with_details(
            update_user_email_10x,
            setup=setup_schema,
            teardown=teardown_all,
            label="Zmiana e-maila usera id=2 (10 razy)"
        )

        # Usuwanie rekordów 10x
        def delete_user_10x():
            for _ in range(10):
                load_users(cur, conn, users[:1])
                cur.execute("DELETE FROM users WHERE id = %s", (users[0]['id'],))
                conn.commit()

        measure_with_details(
            delete_user_10x,
            setup=setup_schema,
            teardown=teardown_all,
            label="Usunięcie użytkownika 10 razy"
        )

        def delete_post_10x():
            load_users(cur, conn, users[:1])
            for _ in range(10):
                load_posts(cur, conn, posts[:1])
                cur.execute("DELETE FROM posts WHERE id = %s", (posts[0]['id'],))
                conn.commit()

        measure_with_details(
            delete_post_10x,
            setup=setup_schema,
            teardown=teardown_all,
            label="Usunięcie posta 10 razy"
        )

        def delete_comment_10x():
            setup_comments()
            for _ in range(10):
                cur.execute("DELETE FROM comments WHERE id = %s", (comments[0]['id'],))
                conn.commit()
                cur.execute("INSERT INTO comments (id, post_id, name, email, body) VALUES (%s, %s, %s, %s, %s)",
                            (comments[0]['id'], comments[0]['post_id'], comments[0]['name'],
                             comments[0]['email'], comments[0]['body']))
                conn.commit()

        measure_with_details(
            delete_comment_10x,
            setup=setup_comments,
            teardown=teardown_all,
            label="Usunięcie komentarza 10 razy"
        )

        # Zliczanie rekordów 10x
        def count_users_10x():
            load_users(cur, conn, users)
            for _ in range(10):
                cur.execute("SELECT COUNT(*) FROM users")
                cur.fetchone()

        measure_with_details(
            count_users_10x,
            setup=setup_schema,
            teardown=teardown_all,
            label="Zliczanie użytkowników 10 razy"
        )

        def count_posts_10x():
            setup_read()
            for _ in range(10):
                cur.execute("SELECT COUNT(*) FROM posts")
                cur.fetchone()

        measure_with_details(
            count_posts_10x,
            setup=setup_read,
            teardown=teardown_all,
            label="Zliczanie postów 10 razy"
        )

        def count_comments_10x():
            setup_comments()
            for _ in range(10):
                cur.execute("SELECT COUNT(*) FROM comments")
                cur.fetchone()

        measure_with_details(              
            count_comments_10x,
            setup=setup_comments,
            teardown=teardown_all,
            label="Zliczanie komentarzy 10 razy"
        )


        print("=" * 60)
        print("6. TESTY WYDAJNOŚCIOWE - srednia")
        print("=" * 60)

               # Zmieniony format wyświetlania wyników
        print(f"Dodawanie użytkowników:    {avg_users:.4f} s")
        print(f"  Dodawanie postów:          {avg_posts:.4f} s")
        print(f"  Dodawanie komentarzy:      {avg_comments:.4f} s")
        print(f"  Masowe dodanie 100 postów: {avg_mass:.4f} s")
        print(f"  Usunięcie 100 komentarzy:  {avg_delete:.4f} s")
        print(f"  Odczyt wszystkich postów:  {avg_read:.4f} s")
        print(f"  Filtrowanie postów user 1: {avg_filter:.4f} s")


if __name__ == '__main__':
    main()
