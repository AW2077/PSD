import json
import os
import sys
from datetime import datetime, timedelta
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from river import tree 

KAFKA_BROKER = 'kafka:29092'
MONGO_URI = 'mongodb://mongodb:27017/'
DB_NAME = 'fraud_database'
COLLECTION_NAME = 'alarms_history'

class AdvancedStatefulDetector(KeyedProcessFunction):
    def __init__(self):
        self.time_window_state = None
        self.model_state = None
        self.mongo_client = None
        self.alarms_collection = None

    def open(self, runtime_context: RuntimeContext):
        window_desc = ValueStateDescriptor("sliding_window_dict_state", Types.PICKLED_BYTE_ARRAY())
        self.time_window_state = runtime_context.get_state(window_desc)
        
        # Stan modelu Hoeffdinga
        model_desc = ValueStateDescriptor("hoeffding_tree_state", Types.PICKLED_BYTE_ARRAY())
        self.model_state = runtime_context.get_state(model_desc)
        
        from pymongo import MongoClient
        self.mongo_client = MongoClient(MONGO_URI)
        self.alarms_collection = self.mongo_client[DB_NAME][COLLECTION_NAME]

    def extract_features(self, data, prev_tx=None):
        lat = data['gps_location']['lat']
        lon = data['gps_location']['lon']
        
        features = {
            'amount': float(data['amount']),
            'available_limit': float(data['available_limit']),
            'lat': float(lat),
            'lon': float(lon),
            'is_poland': 1.0 if (49.0 <= lat <= 55.0 and 14.0 <= lon <= 25.0) else 0.0
        }
        
        if prev_tx:
            prev_lat = prev_tx['gps_location']['lat']
            prev_lon = prev_tx['gps_location']['lon']
            features['lat_diff'] = abs(lat - prev_lat)
            features['lon_diff'] = abs(lon - prev_lon)
            
            t1 = datetime.fromisoformat(data['timestamp'].replace('Z', ''))
            t2 = datetime.fromisoformat(prev_tx['timestamp'].replace('Z', ''))
            features['time_diff'] = (t1 - t2).total_seconds()
        else:
            features['lat_diff'] = 0.0
            features['lon_diff'] = 0.0
            features['time_diff'] = 9999.0
            
        return features

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        try:
            data = json.loads(value)
            alarms = []
            ml_anomaly_reason = ""
            actual_label = int(data.get('is_fraud', 0))

            # PARAMETRY OKNA CZASOWEGO
            WINDOW_DURATION = timedelta(hours=3) 
            current_tx_time = datetime.fromisoformat(data['timestamp'].replace('Z', ''))
            cutoff_time = current_tx_time - WINDOW_DURATION

            # POBRANIE SŁOWNIKA STANU Z FLINKA
            window_dict = self.time_window_state.value()
            if window_dict is None:
                window_dict = {}

            # CZYSZCZENIE OKNA ZE STAROCI I WYZNACZANIE PREV_TX
            active_history = []
            updated_window_dict = {}
            prev_tx = None

            for tx_timestamp_str, tx_data in window_dict.items():
                tx_time = datetime.fromisoformat(tx_timestamp_str.replace('Z', ''))
                
                if tx_time >= cutoff_time:
                    # Transakcja mieści się w oknie
                    active_history.append(tx_data)
                    updated_window_dict[tx_timestamp_str] = tx_data
                    
                    # Szukamy transakcji bezpośrednio poprzedzającej bieżącą
                    if tx_time < current_tx_time:
                        if prev_tx is None:
                            prev_tx = tx_data
                        else:
                            prev_tx_time = datetime.fromisoformat(prev_tx['timestamp'].replace('Z', ''))
                            if tx_time > prev_tx_time:
                                prev_tx = tx_data

            # OBSŁUGA DRZEWA HOEFFDINGA
            model = self.model_state.value()
            if model is None:
                model = tree.HoeffdingTreeClassifier(grace_period=5, split_criterion='info_gain')
            
            features = self.extract_features(data, prev_tx)
            prediction = model.predict_one(features)
            probas = model.predict_proba_one(features)
            model_experience = model._root.total_weight if model._root else 0
            
            # REGUŁY DETEKCJI (ZASADY TRADYCYJNE + ML)
            if prediction == 1 and model_experience >= 5:
                proba_fraud = probas.get(1, 0.0) * 100
                if proba_fraud >= 50.0:
                    alarms.append('HOEFFDING_TREE_ML_ANOMALY')
                    if features['lat_diff'] > 15.0 or features['lon_diff'] > 15.0:
                        ml_anomaly_reason = f"Pewność {proba_fraud:.1f}% -> Model wykrył gwałtowną zmianę współrzędnych GPS."
                    elif float(data['amount']) > float(data['available_limit']):
                        ml_anomaly_reason = f"Pewność {proba_fraud:.1f}% -> Kwota transakcji uderza bezpośrednio w granicę limitu."
                    elif features['time_diff'] < 3.0:
                        ml_anomaly_reason = f"Pewność {proba_fraud:.1f}% -> Wyjątkowo krótki odstęp czasu od poprzedniej operacji."
                    else:
                        ml_anomaly_reason = f"Pewność {proba_fraud:.1f}% -> Złożona anomalia behawioralna w oknie czasowym."

            if float(data['amount']) > float(data['available_limit']):
                alarms.append('LIMIT_EXCEEDED_ANOMALY')

            if prev_tx:
                if features['time_diff'] < 3.0:
                    alarms.append('HIGH_FREQUENCY_ANOMALY')
                if features['lat_diff'] > 15.0 or features['lon_diff'] > 15.0:
                    alarms.append('LOCATION_JUMP_ANOMALY')

                # Średnia krocząca z transakcji w oknie aktywnym
                if len(active_history) >= 2:
                    avg_amount = sum(float(tx['amount']) for tx in active_history) / len(active_history)
                    if float(data['amount']) > (3 * avg_amount):
                        alarms.append('STATISTICAL_AMOUNT_ANOMALY')

            # ZAPIS BIEŻĄCEJ TRANSAKCJI I AKTUALIZACJA STANU
            updated_window_dict[data['timestamp']] = data
            self.time_window_state.update(updated_window_dict)

            if alarms:
                alarm_payload = {
                    "card_id": data['card_id'],
                    "user_id": data['user_id'],
                    "amount": data['amount'],
                    "available_limit": data['available_limit'],
                    "city": data.get('city', 'nieznane'),
                    "prev_city": prev_tx.get('city', 'nieznane') if prev_tx else 'brak',
                    "gps_location": data['gps_location'],
                    "time_since_last": round(features['time_diff'], 2),
                    "anomaly_reasons": alarms,
                    "ml_anomaly_reason": ml_anomaly_reason,
                    "timestamp": data['timestamp'],
                    "was_actually_fraud": actual_label
                }
                try:
                    self.alarms_collection.insert_one(alarm_payload.copy())
                except Exception as e:
                    print(f"Blad MongoDB: {e}", file=sys.stderr)

                yield json.dumps(alarm_payload)

            model.learn_one(features, actual_label)
            self.model_state.update(model)

        except Exception as e:
            print(f"Blad przetwarzania elementu: {e}", file=sys.stderr)

    def close(self):
        if self.mongo_client:
            self.mongo_client.close()

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env.enable_checkpointing(10000)
    checkpoints_dir = os.path.join(project_root, "checkpoints")
    os.makedirs(checkpoints_dir, exist_ok=True)
    env.get_checkpoint_config().set_checkpoint_storage_dir(f"file:///{checkpoints_dir.replace(os.sep, '/')}")

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
        .process(AdvancedStatefulDetector(), output_type=Types.STRING())

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
        env.execute("AdvancedFraudDetectionJobV4")
    except Exception as e:
        print(f"Blad wykonania Flinka: {e}")

if __name__ == '__main__':
    main()