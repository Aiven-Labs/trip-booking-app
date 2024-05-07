import argparse
import sys

from pyflink.common.serialization import SimpleStringSchema # type: ignore
from pyflink.common.watermark_strategy import WatermarkStrategy  # type: ignore
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode # type: ignore
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer # type: ignore
from pyflink.table import StreamTableEnvironment # type: ignore

def main(known_args):
    # Gets execution environment
    stream_execution_environment = StreamExecutionEnvironment.get_execution_environment()
    # Sets execution mod
    stream_execution_environment.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # Sets parallelism
    stream_execution_environment.set_parallelism(1)
    # Registers connectors
    stream_execution_environment.add_jars("file:///flink/usrlib/flink-sql-connector-kafka-1.17.2.jar")
    stream_execution_environment.add_jars("file:///flink/usrlib/flink-connector-jdbc-3.1.2-1.17.jar")
    stream_execution_environment.add_jars("file:///flink/usrlib/postgresql-42.7.3.jar")
    # Gets table environment
    stream_table_environment = StreamTableEnvironment.create(stream_execution_environment)
    # Defines pipeline
    pipeline(
        stream_execution_environment=stream_execution_environment,
        stream_table_environment=stream_table_environment,
        known_args=known_args
    )
    # Submits job
    stream_execution_environment.execute()

def pipeline(stream_execution_environment, stream_table_environment, known_args):
    pipeline_postgres(
        stream_table_environment=stream_table_environment,
        known_args=known_args
    )
    pipeline_kafka(
        stream_execution_environment=stream_execution_environment,
        known_args=known_args
    )

def pipeline_postgres(stream_table_environment, known_args):
    # Registers source
    (
        stream_table_environment
            .execute_sql(
                get_source_postgres(
                    known_args=known_args,
                    table_name="hotels"
                )
            )
    )
    # Adds source, transformation, sink
    (
        stream_table_environment
            .to_data_stream(
                stream_table_environment
                    .from_path("hotels")
            )
            .print()
    )

def pipeline_kafka(stream_execution_environment, known_args):
    # Adds source, transformation, sink
    (
        stream_execution_environment
            .from_source(
                get_source_kafka(
                    known_args=known_args,
                    topic_name="user_activity"
                ),
                WatermarkStrategy.no_watermarks(),
                "Kafka Source"
            )
            .print()
    )

def get_source_postgres(known_args, table_name):
    # Defines connectivity
    return (
        """
        CREATE TABLE {table_name} (
            hotel VARCHAR,
            location VARCHAR,
            price VARCHAR,
            room_size VARCHAR
        ) WITH (
          'connector' = 'jdbc',
          'url' = '{service_uri}',
          'username' = '{username}',
          'password' = '{password}',
          'table-name' = '{table_name}'
        )
        """.format(
            service_uri=known_args.service_uri,
            username=known_args.username,
            password=known_args.password,
            table_name=table_name
        )
    )

def get_source_kafka(known_args, topic_name):
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
    # Adds service uri
    (
        parser
            .add_argument(
                "--service-uri",
                dest="service_uri",
                required=True,
                help="Postgres Service URI."
            )
    )
    # Adds username
    (
        parser
            .add_argument(
                "--username",
                dest="username",
                required=True,
                help="Postgres Username."
            )
    )
    # Adds password
    (
        parser
            .add_argument(
                "--password",
                dest="password",
                required=True,
                help="Postgres Password."
            )
    )
    # Adds bootstrap servers
    (
        parser
            .add_argument(
                "--bootstrap-servers",
                dest="bootstrap_servers",
                required=True,
                help="Kafka Bootstrap Servers."
            )
    )
    # Adds truststore password
    (
        parser
            .add_argument(
                "--ssl-truststore-password",
                dest="ssl_truststore_password",
                required=True,
                help="Kafka SSL Truststore Password."
            )
    )
    # Adds keystore password
    (
        parser
            .add_argument(
                "--ssl-keystore-password",
                dest="ssl_keystore_password",
                required=True,
                help="Kafka SSL Keystore Password."
            )
    )

if __name__ == "__main__":
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
