import json
from kafka import KafkaConsumer
from pymongo import MongoClient

# Konfiguracja
KAFKA_BROKER = 'localhost:9092'
MONGO_URI = 'mongodb://localhost:27017/'

# Połączenie z MongoDB
mongo_client = MongoClient(MONGO_URI)
db = mongo_client['fraud_database']
alarms_collection = db['alarms_history']

consumer = KafkaConsumer(
    'alarms',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Połączono z bazą MongoDB. Oczekuję na alarmy z Kafki...")
print("-" * 80)

try:
    for message in consumer:
        alarm_data = message.value
        
        # Zapis do bazy MongoDB
        alarms_collection.insert_one(alarm_data.copy())
        
        # Wizualizacja w konsoli
        card = alarm_data.get('card_id')
        reasons = ", ".join(alarm_data.get('anomaly_reasons', []))
        print(f"🚨 ALARM | Karta: {card} | Przyczyna: {reasons} | Kwota: {alarm_data.get('amount')} zł")

except KeyboardInterrupt:
    print("Zatrzymano.")
finally:
    consumer.close()
    mongo_client.close()