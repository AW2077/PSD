import json
import os
import sys
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

KAFKA_BROKER = 'localhost:9092'

class StatefulAnomalyDetector(KeyedProcessFunction):
    def __init__(self):
        self.last_location_state = None

    def open(self, runtime_context: RuntimeContext):
        state_desc = ValueStateDescriptor("last_location", Types.STRING())
        self.last_location_state = runtime_context.get_state(state_desc)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        try:
            data = json.loads(value)
            lat, lon = data['gps_location']['lat'], data['gps_location']['lon']
            alarms = []

            if data['amount'] > data['available_limit'] * 0.9:
                alarms.append('HIGH_AMOUNT')

            last_loc_str = self.last_location_state.value()
            if last_loc_str:
                last_loc = json.loads(last_loc_str)
                if abs(lat - last_loc['lat']) > 15.0 or abs(lon - last_loc['lon']) > 15.0:
                    alarms.append('LOCATION_JUMP')

            self.last_location_state.update(json.dumps({'lat': lat, 'lon': lon}))

            if alarms:
                data['anomaly_reasons'] = alarms
                yield json.dumps(data)
        except Exception:
            pass

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # 1. Poprawna konfiguracja interpretera (to musi zostać)
    env.set_python_executable(sys.executable)
    
    # 2. Poprawna metoda dodawania kodu (zamiast set_string na configu)
    # To sprawia, że plik detector.py jest wysyłany do klastra i dostępny dla pracowników
    env.add_python_file(os.path.abspath(__file__))
    
    # 3. Ustawienie paralelizmu
    env.set_parallelism(1)

    # Dynamiczne ładowanie JARa
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    jar_path = os.path.join(project_root, "jars", "flink-sql-connector-kafka-3.2.0-1.19.jar")
    env.add_jars(f"file:///{jar_path.replace(os.sep, '/')}")

    # Źródło danych (Kafka)
    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics('transactions') \
        .set_group_id("flink_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # Przetwarzanie
    alarms_stream = stream \
        .key_by(lambda x: json.loads(x).get('card_id', 'UNKNOWN')) \
        .process(StatefulAnomalyDetector(), output_type=Types.STRING())

    # Ujście danych (Kafka)
    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic('alarms')
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()

    alarms_stream.sink_to(sink)
    
    print("🚀 Flink startuje...")
    env.execute("FraudDetection")

if __name__ == '__main__':
    main()