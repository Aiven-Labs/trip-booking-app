import argparse
import sys

from pyflink.common.serialization import SimpleStringSchema # type: ignore
from pyflink.common.watermark_strategy import WatermarkStrategy  # type: ignore
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode # type: ignore
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer # type: ignore

def main(known_args):
    # Gets execution environment
    stream_execution_environment = StreamExecutionEnvironment.get_execution_environment()
    # Sets execution mod
    stream_execution_environment.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # Sets parallelism
    stream_execution_environment.set_parallelism(1)
    # Registers connectors
    stream_execution_environment.add_jars("file:///flink/usrlib/flink-sql-connector-kafka-1.17.2.jar")
    # Defines pipeline
    pipeline(
        stream_execution_environment=stream_execution_environment,
        known_args=known_args
    )
    # Submits job
    stream_execution_environment.execute()

def pipeline(stream_execution_environment, known_args):
    # Adds source, transformation, sink
    (
        stream_execution_environment
            .from_source(
                get_kafka_source(
                    known_args=known_args,
                    topic_name="user_activity"
                ),
                WatermarkStrategy.no_watermarks(),
                "Kafka Source"
            )
            .print()
    )

def get_kafka_source(known_args, topic_name):
    # Builds source
    return (
        KafkaSource
            .builder()
            .set_bootstrap_servers(known_args.bootstrap_servers)
            .set_topics(topic_name)
            .set_group_id("ververica")
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema())
            .set_property("security.protocol", "SSL")
            .set_property("ssl.truststore.type", "JKS")
            .set_property("ssl.truststore.location", "/flink/usrlib/client.truststore.jks")
            .set_property("ssl.truststore.password", known_args.ssl_truststore_password)
            .set_property("ssl.keystore.type", "PKCS12")
            .set_property("ssl.keystore.location", "/flink/usrlib/client.keystore.p12")
            .set_property("ssl.keystore.password", known_args.ssl_keystore_password)
            .build()
    )

def add_arguments(parser):
    # Adds bootstrap servers
    (
        parser
            .add_argument(
                '--bootstrap-servers',
                dest='bootstrap_servers',
                required=True,
                help='Kafka Bootstrap Servers.'
            )
    )
    # Adds truststore password
    (
        parser
            .add_argument(
                '--ssl-truststore-password',
                dest='ssl_truststore_password',
                required=True,
                help='Kafka SSL Truststore Password.'
            )
    )
    # Adds keystore password
    (
        parser
            .add_argument(
                '--ssl-keystore-password',
                dest='ssl_keystore_password',
                required=True,
                help='Kafka SSL Keystore Password.'
            )
    )

if __name__ == '__main__':
    # Creates parser
    parser = argparse.ArgumentParser()
    # Adds arguments
    add_arguments(parser)
    # Retrieves arguments
    argv = sys.argv[1:]
    # Parses arguments
    known_args, _ = parser.parse_known_args(argv)
    # Runs main
    main(known_args)
