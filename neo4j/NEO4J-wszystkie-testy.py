import time
import json
from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
auth = ("neo4j", "mypassword")

driver = GraphDatabase.driver(uri, auth=auth)

# IMPORT DANYCH DO BAZY

def load_json(path):
    with open(path) as f:
        return json.load(f)

def import_users(tx, users):
    for user in users:
        tx.run("""
            CREATE (:User {
                id: $id,
                name: $name,
                username: $username,
                email: $email
            })
        """, id=user["id"], name=user["name"], username=user["username"], email=user["email"])

def import_posts(tx, posts):
    for post in posts:
        tx.run("""
            MATCH (u:User {id: $userId})
            CREATE (p:Post {
                id: $id,
                title: $title,
                body: $body
            })
            CREATE (u)-[:WROTE]->(p)
        """, id=post["id"], userId=post["userId"], title=post["title"], body=post["body"])

def import_comments(tx, comments):
    for comment in comments:
        tx.run("""
            MATCH (u:User {id: $userId})
            MATCH (p:Post {id: $postId})
            CREATE (c:Comment {
                id: $id,
                name: $name,
                email: $email,
                body: $body
            })
            CREATE (u)-[:WROTE]->(c)
            CREATE (c)-[:ON]->(p)
        """, id=comment["id"], name=comment["name"], email=comment["email"], body=comment["body"],
             userId=(comment["postId"] - 1) // 10,  # uproszczenie — nie ma userId, więc szacujemy
             postId=comment["postId"])

print("🔄 Ładowanie danych...")

users = load_json("./neo4j/users_big.json")
posts = load_json("./neo4j/posts_big.json")
comments = load_json("./neo4j/comments_big.json")

with driver.session() as session:
    
    start = time.time()
    session.execute_write(import_users, users)
    duration = round(time.time() - start, 2)
    print(f"Użytkownicy zaimportowani w {duration} s.")
    
    start = time.time()
    session.execute_write(import_posts, posts)
    duration = round(time.time() - start, 2)
    print(f"Posty zaimportowane w {duration} s.")
    
    start = time.time()
    session.execute_write(import_comments, comments)
    duration = round(time.time() - start, 2)
    print(f"Komentarze zaimportowane w {duration} s.")


# 1 DODAWANIE NOWYCH REKORDÓW

def time_it(label, func):
    print(f"\n️ {label}")
    total = 0
    for i in range(10):
        start = time.time()
        func(i)
        end = time.time()
        duration = round(end - start, 4)
        total += duration
        print(f"  ️ Próba {i+1}: {duration} s")
    print(f"   Średnia: {round(total/10, 4)} s")

def add_user(i):
    with driver.session() as session:
        session.run("""
            CREATE (:User {
                id: $id,
                name: $name,
                username: $username,
                email: $email
            })
        """, id=900000 + i, name=f"Test User {i}", username=f"user{i}", email=f"user{i}@example.com")

def add_post(i):
    with driver.session() as session:
        session.run("""
            MATCH (u:User {id: $userId})
            CREATE (p:Post {
                id: $id,
                title: $title,
                body: $body
            })
            CREATE (u)-[:WROTE]->(p)
        """, id=910000 + i, userId=900000 + i, title=f"Test Post {i}", body=f"Post body {i}")

def add_comment(i):
    with driver.session() as session:
        session.run("""
            MATCH (u:User {id: $userId}), (p:Post {id: $postId})
            CREATE (c:Comment {
                id: $id,
                name: $name,
                email: $email,
                body: $body
            })
            CREATE (u)-[:WROTE]->(c)
            CREATE (c)-[:ON]->(p)
        """, id=920000 + i, userId=900000 + i, postId=910000 + i,
             name=f"Test Comment {i}", email=f"comment{i}@example.com", body=f"Comment body {i}")

# Wykonanie
print("\n" + "=" * 60)
print("1. DODAWANIE NOWYCH REKORDÓW")
print("=" * 60)
time_it("Dodawanie nowych użytkowników", add_user)
time_it("Dodawanie nowych postów", add_post)
time_it("Dodawanie nowych komentarzy", add_comment)

# 2 ODCZYT DANYCH
# a) pełne odczyty
def time_it(label, query):
    print(f"\n {label}")
    total = 0
    for i in range(10):
        start = time.time()
        with driver.session() as session:
            result = session.run(query)
            count = len(list(result))  # wymuszenie wykonania zapytania
        end = time.time()
        duration = round(end - start, 4)
        total += duration
        print(f"  Próba {i+1}: {duration} s (rekordów: {count})")
    print(f"  Średnia: {round(total/10, 4)} s")

# Zapytania
queries = [
    ("Wszyscy użytkownicy", "MATCH (u:User) RETURN u"),
    ("Wszystkie posty", "MATCH (p:Post) RETURN p"),
    ("Wszystkie komentarze", "MATCH (c:Comment) RETURN c")
]

# Wykonanie
print("\n" + "=" * 60)
print("2. ODCZYT DANYCH")
print("=" * 60)
print("\n")
print("a) PEŁNE ODCZYTY")
for label, query in queries:
    time_it(label, query)

# b) filtrowane odczyty
def time_query(label, cypher_template, ids):
    print(f"\n {label}")
    for id_value in ids:
        total = 0
        for i in range(10):
            start = time.time()
            with driver.session() as session:
                result = session.run(cypher_template, id=id_value)
                count = len(list(result))  # wymuszenie wykonania zapytania
            end = time.time()
            duration = round(end - start, 4)
            total += duration
            print(f"  id = {id_value}, próba {i+1}: {duration} s (rekordów: {count})")
        avg = round(total / 10, 4)
        print(f"  Średnia dla id {id_value}: {avg} s")

# Przykładowe ID do testów
user_ids = [123, 321, 999]
post_ids = [1111, 2222, 3333]

# Zapytania Cypher
query_posts_by_user = """
MATCH (u:User {id: $id})-[:WROTE]->(p:Post)
RETURN p
"""

query_comments_by_post = """
MATCH (c:Comment)-[:ON]->(p:Post {id: $id})
RETURN c
"""

# Wykonanie
print("\n")
print("b) FILTROWANE ODCZYTY")
time_query("Posty użytkownika", query_posts_by_user, user_ids)
time_query("Komentarze do posta", query_comments_by_post, post_ids)

# 3 MODYFIKACJA ISTNIEJĄCYCH DANYCH
print("\n" + "=" * 60)
print("3. MODYFIKACJA ISTNIEJĄCYCH DANYCH")
print("=" * 60)

def modify_user(tx, user_id, suffix):
    tx.run("""
        MATCH (u:User {id: $id})
        SET u.name = $name,
            u.username = $username,
            u.email = $email
    """, id=user_id,
           name=f"Modified User {suffix}",
           username=f"mod_user{suffix}",
           email=f"mod{suffix}@example.com")

def run_test(user_ids):
    print("\n Modyfikacja użytkowników")
    for uid in user_ids:
        total = 0
        for i in range(10):
            suffix = f"{uid}_{i}"
            start = time.time()
            with driver.session() as session:
                session.execute_write(modify_user, uid, suffix)
            end = time.time()
            duration = round(end - start, 4)
            total += duration
            print(f"  userId = {uid}, próba {i+1}: {duration} s")
        avg = round(total / 10, 4)
        print(f"  Średnia dla userId {uid}: {avg} s")

# Przykładowe 3 użytkownicy (upewnij się, że istnieją)
user_ids = [100, 200, 300]

run_test(user_ids)

# 4 USUWANIE DOKUMENTÓW
print("\n" + "=" * 60)
print("4. USUWANIE DOKUMENTÓW")
print("=" * 60)

# Dodawanie użytkownika
def add_user(tx, user_id):
    tx.run("""
        CREATE (:User {id: $id, name: "TempUser", username: "temp", email: "temp@ex.com"})
    """, id=user_id)

# Usuwanie użytkownika
def delete_user(tx, user_id):
    tx.run("""
        MATCH (u:User {id: $id})
        DETACH DELETE u
    """, id=user_id)

# Dodawanie posta
def add_post(tx, post_id):
    tx.run("""
        MERGE (u:User {id: 888888}) SET u.name = "Author"
        CREATE (p:Post {id: $id, title: "TempPost", body: "Body"})
        WITH p
        MATCH (u:User {id: 888888})
        CREATE (u)-[:WROTE]->(p)
    """, id=post_id)

# Usuwanie posta
def delete_post(tx, post_id):
    tx.run("""
        MATCH (p:Post {id: $id})
        DETACH DELETE p
    """, id=post_id)

# Dodawanie komentarza
def add_comment(tx, comment_id):
    tx.run("""
        MERGE (u:User {id: 999991}) SET u.name = "Commenter"
        MERGE (p:Post {id: 999991}) SET p.title = "Commented"
        CREATE (c:Comment {
          id: $id,
          name: "ToDelete",
          email: "temp@example.com",
          body: "Temporary comment"
        })
        WITH c
        MATCH (u:User {id: 999991}), (p:Post {id: 999991})
        CREATE (u)-[:WROTE]->(c)
        CREATE (c)-[:ON]->(p)
    """, id=comment_id)

# Usuwanie komentarza
def delete_comment(tx, comment_id):
    tx.run("""
        MATCH (c:Comment {id: $id})
        DETACH DELETE c
    """, id=comment_id)


# Ogólny test usuwania
def run_deletion_test(entity_name, entity_ids, add_fn, delete_fn):
    print(f"\n  Usuwanie {entity_name}")
    for eid in entity_ids:
        total = 0
        for i in range(10):
            with driver.session() as session:
                session.execute_write(add_fn, eid)
            start = time.time()
            with driver.session() as session:
                session.execute_write(delete_fn, eid)
            end = time.time()
            duration = round(end - start, 4)
            total += duration
            print(f"   {entity_name} id = {eid}, próba {i+1}: {duration} s")
        avg = round(total / 10, 4)
        print(f"   Średnia dla {entity_name} id {eid}: {avg} s")


# Lista testowych ID
user_ids = [80001, 80002, 80003]
post_ids = [90001, 90002, 90003]
comment_ids = [99991, 99992, 99993]

# Wykonanie testów
run_deletion_test("użytkownika", user_ids, add_user, delete_user)
run_deletion_test("posta", post_ids, add_post, delete_post)
run_deletion_test("komentarza", comment_ids, add_comment, delete_comment)


# 5 ZLICZANIE I AGREGACJA DANYCH
print("\n" + "=" * 60)
print("5. ZLICZANIE I AGREGACJA DANYCH")
print("=" * 60)

def timed_count(label, cypher_query, parameters=None):
    print(f"\n {label}")
    total = 0
    for i in range(10):
        start = time.time()
        with driver.session() as session:
            result = session.run(cypher_query, parameters or {})
            count = result.single().value()
        end = time.time()
        duration = round(end - start, 4)
        total += duration
        print(f"  ️ Próba {i+1}: {duration} s (wynik: {count})")
    avg = round(total / 10, 4)
    print(f"   Średnia: {avg} s")

# Testy
timed_count(
    "Zliczenie wszystkich użytkowników",
    "MATCH (u:User) RETURN count(u)"
)

timed_count(
    "Zliczenie wszystkich postów",
    "MATCH (p:Post) RETURN count(p)"
)

timed_count(
    "Zliczenie wszystkich komentarzy",
    "MATCH (c:Comment) RETURN count(c)"
)

timed_count(
    "Zliczenie postów użytkownika o id=999",
    "MATCH (:User {id: $id})-[:WROTE]->(p:Post) RETURN count(p)",
    {"id": 999}
)

timed_count(
    "Zliczenie komentarzy do posta o id=1234",
    "MATCH (c:Comment)-[:ON]->(:Post {id: $id}) RETURN count(c)",
    {"id": 1234}
)

# 6 TESTY WYDAJNOŚCIOWE
print("\n" + "=" * 60)
print("6. TESTY WYDAJNOŚCIOWE")
print("=" * 60)

def run_test():
    print("\n Odczyt wszystkich postów")
    total = 0
    for i in range(10):
        start = time.time()
        with driver.session() as session:
            result = session.run("MATCH (p:Post) RETURN count(p)")
            count = result.single().value()
        end = time.time()
        duration = round(end - start, 4)
        total += duration
        print(f"  ️ Próba {i+1}: {duration} s (rekordów: {count})")
    avg = round(total / 10, 4)
    print(f"   Średnia: {avg} s")

run_test()

# 8-------------------------

def run_test():
    print("\n Posty użytkownika o id=999")
    total = 0
    for i in range(10):
        start = time.time()
        with driver.session() as session:
            result = session.run("MATCH (:User {id: $id})-[:WROTE]->(p:Post) RETURN count(p)", {"id": 999})
            count = result.single().value()
        end = time.time()
        duration = round(end - start, 4)
        total += duration
        print(f"   Próba {i+1}: {duration} s (rekordów: {count})")
    avg = round(total / 10, 4)
    print(f"   Średnia: {avg} s")


run_test()

# 9 ---------------------

def add_100_posts(tx, base_id):
    for i in range(100):
        tx.run("""
            CREATE (p:Post {
                id: $id,
                title: $title,
                body: $body
            })
        """, id=base_id + i, title=f"Bulk title {i}", body=f"Bulk content {i}")

def run_test():
    print("\n Masowe dodanie 100 postów")
    total = 0
    base_id = 999900  # aby nie kolidować z istniejącymi
    for i in range(10):
        start = time.time()
        with driver.session() as session:
            session.execute_write(add_100_posts, base_id + i*100)
        end = time.time()
        duration = round(end - start, 4)
        total += duration
        print(f"   Próba {i+1}: {duration} s (id: {base_id + i*100}–{base_id + i*100 + 99})")
    avg = round(total / 10, 4)
    print(f"   Średnia: {avg} s")


run_test()

#10 -----------------------------
def add_100_comments(tx, ids):
    tx.run("""
        MERGE (u:User {id: 999991}) SET u.name = "BulkUser"
        MERGE (p:Post {id: 999991}) SET p.title = "BulkPost"
    """)
    for cid in ids:
        tx.run("""
            CREATE (c:Comment {
                id: $id,
                name: $name,
                email: $email,
                body: $body
            })
            WITH c
            MATCH (u:User {id: 999991}), (p:Post {id: 999991})
            CREATE (u)-[:WROTE]->(c)
            CREATE (c)-[:ON]->(p)
        """, id=cid, name=f"Comment {cid}", email=f"comment{cid}@example.com", body="Performance test")


def delete_100_comments(tx, ids):
    tx.run("""
        MATCH (c:Comment) WHERE c.id IN $ids
        DETACH DELETE c
    """, ids=ids)


# ======= MASOWE DODAWANIE KOMENTARZY (10x100) =======
def run_add_comments_test():
    print("\n Masowe dodanie 100 komentarzy")
    total_insert = 0
    base_id = 999800
    ids_list = [[base_id + i for i in range(j*100, (j+1)*100)] for j in range(10)]

    for i, ids in enumerate(ids_list):
        start = time.time()
        with driver.session() as session:
            session.execute_write(add_100_comments, ids)
        end = time.time()
        duration = round(end - start, 6)
        total_insert += duration
        print(f"  Próba {i+1}: {duration} s (komentarze: {ids[0]}–{ids[-1]})")

    avg_insert = round(total_insert / 10, 6)
    print(f"   Średnia dodawania: {avg_insert} s")

# ======= MASOWE USUWANIE KOMENTARZY (10x100) =======
def run_delete_comments_test():
    print("\n Masowe usunięcie 100 komentarzy")
    total_delete = 0
    base_id = 999800
    ids_list = [[base_id + i for i in range(j*100, (j+1)*100)] for j in range(10)]

    for i, ids in enumerate(ids_list):
        start = time.time()
        with driver.session() as session:
            session.execute_write(delete_100_comments, ids)
        end = time.time()
        duration = round(end - start, 6)
        total_delete += duration
        print(f"  ️Próba {i+1}: {duration} s (komentarze: {ids[0]}–{ids[-1]})")

    avg_delete = round(total_delete / 10, 6)
    print(f"   Średnia usuwania: {avg_delete} s")

# ======= URUCHOM TESTY =======
run_add_comments_test()
run_delete_comments_test()



'''
def delete_100_comments(tx, ids):
    tx.run("""
        MATCH (c:Comment) WHERE c.id IN $ids
        DETACH DELETE c
    """, ids=ids)

def run_test():
    print("\n Masowe usunięcie 100 komentarzy")
    total = 0
    base_id = 999800  # przygotuj te komentarze wcześniej, np. kopiując istniejące
    ids_list = [[base_id + i for i in range(j*100, (j+1)*100)] for j in range(10)]

    for i, ids in enumerate(ids_list):
        # opcjonalnie: wcześniej dodaj te komentarze w osobnym skrypcie
        start = time.time()
        with driver.session() as session:
            session.execute_write(delete_100_comments, ids)
        end = time.time()
        duration = round(end - start, 4)
        total += duration
        print(f"   Próba {i+1}: {duration} s (id: {ids[0]}–{ids[-1]})")
    avg = round(total / 10, 4)
    print(f"   Średnia: {avg} s")



run_test()
'''
driver.close()