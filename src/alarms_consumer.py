import json
from kafka import KafkaConsumer

# konfiguracja
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'alarms'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Uruchomiono monitor alarmow...")

try:
    for message in consumer:
        alarm_data = message.value
        
        
        # przypisanie pol z jsona
        card = alarm_data.get('card_id')
        reasons_list = alarm_data.get('anomaly_reasons', [])
        reasons_str = ", ".join(reasons_list)
        amount = alarm_data.get('amount')
        limit = alarm_data.get('available_limit')
        city = alarm_data.get('city', 'nieznane')
        prev_city = alarm_data.get('prev_city', 'nieznane')
        time_diff = alarm_data.get('time_since_last')
        gps = alarm_data.get('gps_location', {})
        lat = gps.get('lat', 'brak danych')
        lon = gps.get('lon', 'brak danych')
        
        print(f"ALARM | uwaga, podejrzana aktywnosc na karcie {card}")
        print(f"      | powod: {reasons_str}")
        print(f"      | kwota transakcji: {amount} pln")
        print(f"      | lokalizacja GPS: lat {lat}, lon {lon}")
        
        # weryfikacja szczegolowa pod typy anomalii
        if 'LIMIT_EXCEEDED_ANOMALY' in reasons_list:
            print(f"      | limit karty: {limit} pln (przekroczenie o {round(amount - limit, 2)} pln)")
            
        if 'STATISTICAL_AMOUNT_ANOMALY' in reasons_list:
            print(f"      | uwaga: kwota ponad 3x wieksza niz historyczna srednia wydatkow")
            
        if 'LOCATION_JUMP_ANOMALY' in reasons_list:
            print(f"      | lokalizacja: wykryto nagly skok geograficzny z {prev_city} do {city}")
            
        if 'HIGH_FREQUENCY_ANOMALY' in reasons_list:
            print(f"      | czas od ostatniej operacji: {time_diff} sek.")
            
        if 'HOEFFDING_TREE_ML_ANOMALY' in reasons_list:
                    print(f"      | [ML] UWAGA: Drzewo Hoeffdinga sklasyfikowało profil transakcji jako podejrzany!")
                    
        print("-" * 60)

except KeyboardInterrupt:
    print("zatrzymano monitor alarmow.")
finally:
    consumer.close()