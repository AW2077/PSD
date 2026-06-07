import json
from kafka import KafkaConsumer

# konfiguracja
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'transactions'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

print("Uruchomiono testowy czytnik transakcji, nasluchiwanie...")

try:
    for message in consumer:
        data = message.value
        card = data.get('card_id')
        user = data.get('user_id')
        amount = data.get('amount')
        limit = data.get('available_limit')
        lat = data.get('gps_location', {}).get('lat')
        lon = data.get('gps_location', {}).get('lon')
        
        # surowa wizualizacja ruchu
        print(f"transakcja | karta: {card} | uzytkownik: {user} | "
              f"kwota: {amount} pln (limit: {limit}) | "
              f"gps: [{lat}, {lon}]")
              
except KeyboardInterrupt:
    print("zatrzymano czytnik testowy.")
finally:
    consumer.close()