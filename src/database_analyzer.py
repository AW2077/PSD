from pymongo import MongoClient

# konfiguracja polaczenia
MONGO_URI = 'mongodb://localhost:27017/'
client = MongoClient(MONGO_URI)
db = client['fraud_database']
collection = db['alarms_history']

print("rozpoczynam analize statystyczna bazy mongodb...")
print("=" * 50)

# 1. ogolna liczba zarejestrowanych oszustw
total_alarms = collection.count_documents({})
print(f"laczna liczba wykrytych anomalii: {total_alarms}")

if total_alarms == 0:
    print("baza jest pusta. uruchom najpierw symulator i detektor.")
    exit()

# 2. statystyka typow anomalii (agregacja)
print("\nrozklad procentowy typow oszustw:")
pipeline_types = [
    {"$unwind": "$anomaly_reasons"},
    {"$group": {"_id": "$anomaly_reasons", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
for result in collection.aggregate(pipeline_types):
    reason = result['_id']
    count = result['count']
    percentage = round((count / total_alarms) * 100, 2)
    print(f"- {reason}: {count} zdarzen ({percentage}%)")

# 3. statystyka miast wysokiego ryzyka
print("\nmiasta z najwieksza liczba naduzyc:")
pipeline_cities = [
    {"$group": {"_id": "$city", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 3}
]
for result in collection.aggregate(pipeline_cities):
    print(f"- {result['_id']}: {result['count']} alarmow")

# 4. analiza finansowa
pipeline_money = [
    {"$group": {
        "_id": None, 
        "avg_fraud": {"$avg": "$amount"},
        "max_fraud": {"$max": "$amount"},
        "total_stolen": {"$sum": "$amount"}
    }}
]
money_stats = list(collection.aggregate(pipeline_money))[0]
print("\nstatystyki finansowe:")
print(f"- srednia kwota oszustwa: {round(money_stats['avg_fraud'], 2)} pln")
print(f"- najwieksza zablokowana transakcja: {round(money_stats['max_fraud'], 2)} pln")
print(f"- laczna uratowana kwota: {round(money_stats['total_stolen'], 2)} pln")

print("=" * 50)
client.close()