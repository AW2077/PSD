import json
import time
import random
import os
from kafka import KafkaProducer
from datetime import datetime

# konfiguracja brokera
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'transactions'

polish_cities = [
    {"name": "Warszawa", "lat": 52.2297, "lon": 21.0122},
    {"name": "Krakow", "lat": 50.0647, "lon": 19.9450},
    {"name": "Lodz", "lat": 51.7592, "lon": 19.4560},
    {"name": "Wroclaw", "lat": 51.1079, "lon": 17.0385},
    {"name": "Poznan", "lat": 52.4064, "lon": 16.9252},
    {"name": "Gdansk", "lat": 54.3520, "lon": 18.6466},
    {"name": "Szczecin", "lat": 53.4285, "lon": 14.5528},
    {"name": "Bydgoszcz", "lat": 53.1235, "lon": 18.0084},
    {"name": "Lublin", "lat": 51.2465, "lon": 22.5684},
    {"name": "Bialystok", "lat": 53.1325, "lon": 23.1688},
    {"name": "Katowice", "lat": 50.2649, "lon": 19.0238},
    {"name": "Gdynia", "lat": 54.5189, "lon": 18.5305},
    {"name": "Czestochowa", "lat": 50.8118, "lon": 19.1203},
    {"name": "Radom", "lat": 51.4027, "lon": 21.1471},
    {"name": "Torun", "lat": 53.0138, "lon": 18.5984}
]

#lista zagranicznych miast (anomalie lokalizacyjne)
foreign_cities = [
    {"name": "Nowy Jork", "lat": 40.7128, "lon": -74.0060},
    {"name": "Tokio", "lat": 35.6762, "lon": 139.6503},
    {"name": "Sydney", "lat": -33.8688, "lon": 151.2093},
    {"name": "Kapsztad", "lat": -33.9249, "lon": 18.4241},
    {"name": "Rio de Janeiro", "lat": -22.9068, "lon": -43.1729},
    {"name": "Londyn", "lat": 51.5074, "lon": -0.1278},
    {"name": "Paryz", "lat": 48.8566, "lon": 2.3522},
    {"name": "Pekin", "lat": 39.9042, "lon": 116.4074},
    {"name": "Dubaj", "lat": 25.2048, "lon": 55.2708},
    {"name": "Los Angeles", "lat": 34.0522, "lon": -118.2437},
    {"name": "Moskwa", "lat": 55.7558, "lon": 37.6173},
    {"name": "Bombaj", "lat": 19.0760, "lon": 72.8777},
    {"name": "Kair", "lat": 30.0444, "lon": 31.2357},
    {"name": "Toronto", "lat": 43.6510, "lon": -79.3470},
    {"name": "Buenos Aires", "lat": -34.6037, "lon": -58.3816}
]

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

DB_FILE = 'cards_database.json'

#baza kart i wlascicieli(zeby dana karta zawsze nalezala do tego samego wlasciciela)
if os.path.exists(DB_FILE):
    print("Wczytywanie bazy kart z pliku...")
    with open(DB_FILE, 'r') as f:
        cards_data = json.load(f)
else:
    print("Generowanie nowej bazy kart...")
    users = [f"USER_{i:04d}" for i in range(2000)]
    cards_data = []

    for i in range(10000):
        cards_data.append({
            "card_id": f"CARD_{i:05d}",
            "user_id": random.choice(users),
            "limit": random.choice([2000, 5000, 10000, 20000]),
            "home_city": random.choice(polish_cities)
        })
    
    # zapis bazy do pliku
    with open(DB_FILE, 'w') as f:
        json.dump(cards_data, f, indent=4)


print("Uruchomiono symulator transakcji...")

try:
    while True:
        card = random.choice(cards_data)
        
        # 80% transakcji odbywa się w mieście domowym, 20% w losowym innym polskim mieście
        if random.random() < 0.20:
            location = random.choice(polish_cities)
        else:
            location = card['home_city']
            
        amount = round(random.uniform(10.0, 500.0), 2)
        is_frequency_burst = False
        is_fraud_flag = 0
        
        # 20% szansy na wystapienie anomalii
        if random.random() < 0.20:
            
            # waga wystapienia anomalii
            anomaly_type = random.choices(['AMOUNT', 'LOCATION', 'FREQUENCY'], weights=[0.38, 0.60, 0.02])[0]
            is_fraud_flag = 1
            
            if anomaly_type == 'AMOUNT':
                amount = round(card['limit'] * random.uniform(1.1, 1.5), 2)
            elif anomaly_type == 'LOCATION':
                location = random.choice(foreign_cities)
            elif anomaly_type == 'FREQUENCY':
                is_frequency_burst = True


        #lekkie korekty wspolrzednych, zeby wszystkie nie byly takie same        
        lat = location['lat'] + random.uniform(-0.02, 0.02)
        lon = location['lon'] + random.uniform(-0.02, 0.02)
                
        transaction = {
            "card_id": card['card_id'],
            "user_id": card['user_id'],
            "city": location['name'],
            "gps_location": {
                "lat": round(lat, 4), 
                "lon": round(lon, 4)
            },
            "amount": amount,
            "available_limit": card['limit'],
            "timestamp": datetime.utcnow().isoformat(),
            "is_fraud": is_fraud_flag
        }
        
        if is_frequency_burst:
            producer.send(TOPIC, value=transaction)
            time.sleep(0.1)
            transaction['timestamp'] = datetime.utcnow().isoformat()
        
        producer.send(TOPIC, value=transaction)
        time.sleep(0.05)
        
except KeyboardInterrupt:
    print("zatrzymano symulator.")
finally:
    producer.close()