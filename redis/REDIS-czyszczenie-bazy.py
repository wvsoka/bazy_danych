import redis

# Połączenie z Redis (domyślnie localhost:6379)
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Usunięcie wszystkich danych z Redis
print(" Usuwam wszystkie dane z Redis...")
r.flushall()

# Sprawdzenie liczby pozostałych kluczy
remaining = r.dbsize()
if remaining == 0:
    print(" Wszystkie dane zostały usunięte. Redis jest pusty.")
else:
    print(f" Redis nadal zawiera {remaining} kluczy.")
