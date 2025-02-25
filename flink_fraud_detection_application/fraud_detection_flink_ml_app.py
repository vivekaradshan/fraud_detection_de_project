import json
# import numpy as np
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, RollingPolicy, OutputFileConfig
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema, Encoder
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.common.execution_config import ExecutionConfig
from pyflink.common.watermark_strategy import WatermarkStrategy
import os

# Set up Flink environment
env = StreamExecutionEnvironment.get_execution_environment()

env.add_jars("C:\\Users\\Viveka\\Viveka\\Upskill\\2025\\Fraud detection project\\fraud_detection_de_project\\flink_fraud_detection_application\\jars\\flink-sql-connector-kafka-3.3.0-1.20.jar")
jar_path="C:\\Users\\Viveka\\Viveka\\Upskill\\2025\\Fraud detection project\\fraud_detection_de_project\\flink_fraud_detection_application\\jars\\flink-sql-connector-kafka-3.3.0-1.20.jar"

if os.path.exists(jar_path):
    print("JAR file found")
else:
    print("JAR file NOT found")

kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers('localhost:9092') \
    .set_topics('card_transaction') \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

ds = env.from_source(kafka_source, watermark_strategy=WatermarkStrategy.no_watermarks(), source_name="Kafka Source")
ds.map(lambda transaction: json.loads(transaction)).print()

env.execute("Real-Time Fraud Detection with Scaling")