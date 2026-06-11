import json
import os
import sys
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pymongo import MongoClient

# Konfiguracja adresów dla środowiska wewnątrz Dockera
KAFKA_BROKER = 'kafka:29092'
MONGO_URI = 'mongodb://mongodb:27017/'
DB_NAME = 'fraud_database'
COLLECTION_NAME = 'alarms_history'

class StatefulAnomalyDetector(KeyedProcessFunction):
    def __init__(self):
        self.stats_state = None 
        self.mongo_client = None
        self.alarms_collection = None

    def open(self, runtime_context: RuntimeContext):
        # Definicja stanu: [suma_kwot, licznik, lat, lon, timestamp, miasto]
        state_desc = ValueStateDescriptor("stats", Types.PICKLED_BYTE_ARRAY())
        self.stats_state = runtime_context.get_state(state_desc)
        
        # Nawiązanie połączenia z bazą MongoDB
        self.mongo_client = MongoClient(MONGO_URI)
        db = self.mongo_client[DB_NAME]
        self.alarms_collection = db[COLLECTION_NAME]

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        try:
            data = json.loads(value)
            amount = data['amount']
            limit = data['available_limit']
            lat = data['gps_location']['lat']
            lon = data['gps_location']['lon']
            city = data.get('city', 'nieznane')
            timestamp_str = data['timestamp']
            
            # Konwersja czasu do obiektu datetime
            current_time = datetime.fromisoformat(timestamp_str.replace('Z', ''))

            # Pobranie historii danej karty ze stanu
            current_stats = self.stats_state.value()
            
            if current_stats is None:
                is_foreign_first_tx = not (49.0 <= lat <= 55.0 and 14.0 <= lon <= 25.0)
                
                # Zapisujemy stan tylko, jeśli pierwsza transakcja była w Polsce
                if not is_foreign_first_tx:
                    self.stats_state.update([amount, 1, lat, lon, timestamp_str, city])
                return

            total_amount, count, prev_lat, prev_lon, prev_time_str, prev_city = current_stats
            prev_time = datetime.fromisoformat(prev_time_str.replace('Z', ''))

            alarms = []
            time_diff = (current_time - prev_time).total_seconds()

            
            # 1. Przekroczenie limitu kwotowego
            if amount > limit:
                alarms.append('LIMIT_EXCEEDED_ANOMALY')

            # 2. Niemożliwa częstotliwość (np. poniżej 3 sekund)
            if time_diff < 3.0:
                alarms.append('HIGH_FREQUENCY_ANOMALY')

            # 3. Zabezpieczenie statystyczne przed wydatkiem 3x większym niż średnia
            if count > 5:
                avg_amount = total_amount / count
                if amount > (3 * avg_amount):
                    alarms.append('STATISTICAL_AMOUNT_ANOMALY')

            # 4. Skok lokalizacji (Location Jump)
            if abs(lat - prev_lat) > 15.0 or abs(lon - prev_lon) > 15.0:
                alarms.append('LOCATION_JUMP_ANOMALY')

            # --- ZAPIS ALARMU I PRZEKAZANIE DALEJ ---
            if alarms:
                alarm_payload = {
                    "card_id": data['card_id'],
                    "user_id": data['user_id'],
                    "amount": amount,
                    "available_limit": limit,
                    "city": city,
                    "prev_city": prev_city,
                    "gps_location": data['gps_location'],
                    "time_since_last": round(time_diff, 2),
                    "anomaly_reasons": alarms,
                    "timestamp": timestamp_str
                }
                
                # Zapisujemy incydent do MongoDB
                try:
                    self.alarms_collection.insert_one(alarm_payload.copy())
                except Exception as e:
                    print(f"Blad MongoDB: {e}", file=sys.stderr)

                # Wypychamy alarm dalej na temat alarms
                yield json.dumps(alarm_payload)

            else:
                # Aktualizujemy stan tylko i wyłącznie wtedy, gdy transakcja BYŁA LEGALNA
                self.stats_state.update([
                    total_amount + amount,
                    count + 1,
                    lat,
                    lon,
                    timestamp_str,
                    city
                ])

        except Exception as e:
            # Wyrzuca błędy do logów Flinka
            print(f"Blad przetwarzania elementu: {e}", file=sys.stderr)

    def close(self):
        # Sprzątanie połączeń
        if self.mongo_client:
            self.mongo_client.close()

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)


    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Checkpointy (Fault Tolerance)
    env.enable_checkpointing(10000)
    checkpoints_dir = os.path.join(project_root, "checkpoints")
    os.makedirs(checkpoints_dir, exist_ok=True)
    env.get_checkpoint_config().set_checkpoint_storage_dir(f"file:///{checkpoints_dir.replace(os.sep, '/')}")

    # Załadowanie pliku JAR dla Kafki
    jar_path = os.path.join(project_root, "jars", "flink-sql-connector-kafka-3.2.0-1.19.jar")
    env.add_jars(f"file:///{jar_path.replace(os.sep, '/')}")

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics('transactions') \
        .set_group_id("fraud_detector") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka")

    alarms = stream \
        .key_by(lambda x: json.loads(x).get('card_id')) \
        .process(StatefulAnomalyDetector(), output_type=Types.STRING())

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic('alarms')
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ).build()

    alarms.sink_to(sink)
    
    try:
        env.execute("FraudDetectionJob")
    except Exception as e:
        print(f"Blad wykonania Flinka: {e}")

if __name__ == '__main__':
    main()