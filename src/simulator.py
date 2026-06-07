import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'transactions'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

cards = {f"CARD_{i:05d}": {"limit": random.choice([2000, 5000, 10000]), "lat": 52.22, "lon": 21.01} for i in range(1, 1000)}

print("🚀 Uruchamiam symulator transakcji...")

try:
    while True:
        card_id = random.choice(list(cards.keys()))
        card = cards[card_id]
        
        amount = round(random.uniform(5.0, card['limit'] * 0.1), 2)
        lat = card['lat'] + random.uniform(-0.01, 0.01)
        lon = card['lon'] + random.uniform(-0.01, 0.01)
        
        # Wstrzykiwanie anomalii
        if random.random() < 0.05:
            if random.choice([True, False]):
                amount = round(card['limit'] * random.uniform(1.1, 1.5), 2) # Przekroczenie limitu
            else:
                lat, lon = random.uniform(-90.0, 90.0), random.uniform(-180.0, 180.0) # Skok lokalizacji

        card['lat'], card['lon'] = lat, lon

        transaction = {
            "card_id": card_id,
            "amount": amount,
            "available_limit": card["limit"],
            "gps_location": {"lat": round(lat, 4), "lon": round(lon, 4)},
            "timestamp": datetime.utcnow().isoformat()
        }
        producer.send(TOPIC, value=transaction)
        time.sleep(0.05)

except KeyboardInterrupt:
    print("Zatrzymano.")
finally:
    producer.close()