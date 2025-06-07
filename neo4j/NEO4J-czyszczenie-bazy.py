from neo4j import GraphDatabase

# Dane logowania
uri = "bolt://localhost:7687"
auth = ("neo4j", "test1234")

driver = GraphDatabase.driver(uri, auth=auth)

with driver.session() as session:
    session.run("MATCH (n) DETACH DELETE n")
    print("🧹 Wszystkie dane zostały usunięte z bazy.")

driver.close()
