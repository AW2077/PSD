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

# konfiguracja brokera
KAFKA_BROKER = 'localhost:9092'

class StatefulAnomalyDetector(KeyedProcessFunction):
    def __init__(self):
        self.stats_state = None 

    def open(self, runtime_context: RuntimeContext):
        # stan: [suma_kwot, licznik, ostatnia_lat, ostatnia_lon, czas_ostatniej_transakcji, ostatnie_miasto]
        state_desc = ValueStateDescriptor("stats", Types.PICKLED_BYTE_ARRAY())
        self.stats_state = runtime_context.get_state(state_desc)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        try:
            data = json.loads(value)
            amount = data['amount']
            limit = data['available_limit']
            lat, lon = data['gps_location']['lat'], data['gps_location']['lon']
            city = data.get('city', 'nieznane')
            current_time_str = data['timestamp']
            current_time = datetime.fromisoformat(current_time_str)
            
            alarms = []
            time_diff_seconds = None
            
            # 1. twarda regula limitu bankowego
            if amount > limit:
                alarms.append('LIMIT_EXCEEDED_ANOMALY')

            # pobranie historii z pamieci flinka
            state = self.stats_state.value()
            
            if state is not None:
                sum_amt, count, last_lat, last_lon, last_time_str, last_city = state
                last_time = datetime.fromisoformat(last_time_str)
                
                # 2. detekcja czestotliwosci (okno 3 sekundy)
                time_diff_seconds = (current_time - last_time).total_seconds()
                if time_diff_seconds < 3.0:
                    alarms.append('HIGH_FREQUENCY_ANOMALY')

                # 3. detekcja statystyczna (kwota > 3x srednia)
                if count > 5:
                    avg = sum_amt / count
                    if amount > (avg * 3.0):
                        alarms.append('STATISTICAL_AMOUNT_ANOMALY')

                # 4. detekcja skoku lokalizacji (tolerancja > 15 stopni)
                if abs(lat - last_lat) > 15.0 or abs(lon - last_lon) > 15.0:
                    alarms.append('LOCATION_JUMP_ANOMALY')
            else:
                # wartosci poczatkowe dla nowych kart
                sum_amt = 0.0
                count = 0
                last_city = city

            # aktualizacja bezpiecznego stanu (tylko czyste transakcje ucza model)
            if not alarms:
                self.stats_state.update([sum_amt + amount, count + 1, lat, lon, current_time_str, city])

            # wyslanie alarmow do kafki
            if alarms:
                data['anomaly_reasons'] = alarms
                data['prev_city'] = last_city
                if time_diff_seconds is not None:
                    data['time_since_last'] = round(time_diff_seconds, 2)
                else:
                    data['time_since_last'] = 0.0
                yield json.dumps(data)
                
        except Exception:
            pass

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_python_executable(sys.executable)
    env.add_python_file(os.path.abspath(__file__))
    env.set_parallelism(1)

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # --- konfiguracja pamieci trwalej (checkpointing) ---
    # zapis stanu co 10 sekund
    env.enable_checkpointing(10000)
    checkpoints_dir = os.path.join(project_root, "checkpoints")
    os.makedirs(checkpoints_dir, exist_ok=True)
    env.get_checkpoint_config().set_checkpoint_storage_dir(f"file:///{checkpoints_dir.replace(os.sep, '/')}")
    # ----------------------------------------------------

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
    print("Uruchomiono detektor anomalii...")
    
    try:
        env.execute("FraudDetectionJob")
    except KeyboardInterrupt:
        pass
    except Exception:
        pass

if __name__ == '__main__':
    main()