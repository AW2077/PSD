from pymongo import MongoClient

# konfiguracja
MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'fraud_database'
COLLECTION_NAME = 'alarms_history'

def run_analysis():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    alarms = db[COLLECTION_NAME]
    
    total_alarms = alarms.count_documents({})
    
    if total_alarms == 0:
        print("Baza danych jest pusta. Uruchom najpierw symulator i detektor.")
        return

    print("=" * 50)
    print(" RAPORT ANALIZY SYSTEMU FRAUD DETECTION")
    print("=" * 50)
    print(f"Całkowita liczba zarejestrowanych alarmów: {total_alarms}\n")

    # 1. Agregacja typów anomalii
    print("--- ROZKŁAD TYPÓW ANOMALII ---")
    pipeline_reasons = [
        {"$unwind": "$anomaly_reasons"},
        {"$group": {"_id": "$anomaly_reasons", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    for doc in alarms.aggregate(pipeline_reasons):
        print(f" * {doc['_id']}: {doc['count']}")
        
    # 2. Hotspoty - Top 5 miast z największą liczbą alarmów
    print("\n--- TOP 5 MIAST (FRAUD HOTSPOTY OGÓŁEM) ---")
    pipeline_cities = [
        {"$group": {"_id": "$city", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    for doc in alarms.aggregate(pipeline_cities):
        miasto = doc['_id'] if doc['_id'] else "Nieznane"
        print(f" * {miasto}: {doc['count']} incydentów")

    # 3. Top 5 miast docelowych dla LOCATION_JUMP_ANOMALY
    print("\n--- TOP 5 MIAST (SKOKI LOKALIZACYJNE) ---")
    pipeline_jumps = [
        {"$match": {"anomaly_reasons": "LOCATION_JUMP_ANOMALY"}},
        {"$group": {"_id": "$city", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    for doc in alarms.aggregate(pipeline_jumps):
        miasto = doc['_id'] if doc['_id'] else "Nieznane"
        print(f" * {miasto}: {doc['count']} skoków")

    # 4. Top 5 najbardziej atakowanych kart
    print("\n--- TOP 5 ATAKOWANYCH KART ---")
    pipeline_cards = [
        {"$group": {"_id": "$card_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    for doc in alarms.aggregate(pipeline_cards):
        print(f" * {doc['_id']}: {doc['count']} alarmów")

    # 5. Średnia kwota zablokowanej transakcji
    print("\n--- STATYSTYKI KWOTOWE ---")
    pipeline_amounts = [
        {"$group": {
            "_id": None, 
            "avg_amount": {"$avg": "$amount"},
            "max_amount": {"$max": "$amount"},
            "total_blocked": {"$sum": "$amount"}
        }}
    ]
    amount_stats = list(alarms.aggregate(pipeline_amounts))
    if amount_stats:
        stats = amount_stats[0]
        print(f" * Średnia kwota w alarmie: {round(stats['avg_amount'], 2)} PLN")
        print(f" * Rekordowa zablokowana kwota: {round(stats['max_amount'], 2)} PLN")
        print(f" * Łączna suma uchroniona przed kradzieżą: {round(stats['total_blocked'], 2)} PLN")

    print("\n" + "=" * 50)
    client.close()

if __name__ == '__main__':
    run_analysis()