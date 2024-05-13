import argparse
import sys

from pyflink.common.typeinfo import Types  # type: ignore
from pyflink.common.types import RowKind  # type: ignore
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode  # type: ignore
from pyflink.datastream.connectors import DeliveryGuarantee  # type: ignore
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema  # type: ignore
from pyflink.datastream.formats.json import JsonRowSerializationSchema  # type: ignore
from pyflink.datastream.functions import FilterFunction, MapFunction # type: ignore
from pyflink.table import StreamTableEnvironment # type: ignore

type_info_hotel = (
    Types
        .ROW_NAMED(
            [
                "hotel",
                "location",
                "room_size",
                "price"
            ],
            [
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING()
            ]
        )
)

serialization_schema_hotel = (
    JsonRowSerializationSchema
        .builder()
        .with_type_info(type_info_hotel)
        .build()
)

class IsNotUpdateBefore(FilterFunction):

    def filter(self, value):
        return value.get_row_kind() is not RowKind.UPDATE_BEFORE

class Hotel(MapFunction):

    def map(self, hotel):
        if (hotel.get_row_kind() is RowKind.INSERT):
            return hotel
        elif (hotel.get_row_kind() is RowKind.UPDATE_AFTER):
            return hotel
        elif (hotel.get_row_kind() is RowKind.DELETE):
            return hotel

def add_arguments(parser):
    # Adds hostname
    (
        parser
            .add_argument(
                "--hostname",
                dest="hostname",
                required=True,
                help="Postgres Hostname."
            )
    )
    # Adds port
    (
        parser
            .add_argument(
                "--port",
                dest="port",
                required=True,
                help="Postgres Port."
            )
    )
    # Adds database name
    (
        parser
            .add_argument(
                "--database-name",
                dest="database_name",
                required=True,
                help="Postgres Database Name."
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
    stream_execution_environment = StreamExecutionEnvironment.get_execution_environment()
    # Sets execution mod
    stream_execution_environment.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # Sets parallelism
    stream_execution_environment.set_parallelism(1)
    # Registers connectors
    stream_execution_environment.add_jars("file:///flink/usrlib/flink-sql-connector-kafka-1.17.2.jar")
    stream_execution_environment.add_jars("file:///flink/usrlib/flink-sql-connector-postgres-cdc-3.0.1.jar")
    # Gets table environment
    stream_table_environment = StreamTableEnvironment.create(stream_execution_environment)
    # Registers source
    (
        stream_table_environment
            .execute_sql(
                """
                CREATE TABLE hotels (
                    hotel VARCHAR,
                    location VARCHAR,
                    room_size VARCHAR,
                    price DECIMAL
                ) WITH (
                'connector' = 'postgres-cdc',
                'hostname' = '{hostname}',
                'port' = '{port}',
                'username' = '{username}',
                'password' = '{password}',
                'database-name' = '{database_name}',
                'schema-name' = 'public',
                'table-name' = 'hotels',
                'slot.name' = 'flink',
                'decoding.plugin.name' = 'pgoutput'
                )
                """.format(
                    hostname=known_args.hostname,
                    port=known_args.port,
                    username=known_args.username,
                    password=known_args.password,
                    database_name=known_args.database_name
                )
            )
    )
    (
        stream_table_environment
            .to_changelog_stream(
                stream_table_environment
                    .sql_query(
                        """
                        SELECT hotel, location, room_size, CAST(price AS STRING) AS price
                        FROM hotels
                        """
                    )
            )
            .filter(IsNotUpdateBefore())
            .map(Hotel(), output_type=type_info_hotel)
            .sink_to(
                KafkaSink
                    .builder()
                    .set_bootstrap_servers(known_args.bootstrap_servers)
                    .set_record_serializer(
                        KafkaRecordSerializationSchema
                            .builder()
                            .set_topic("hotels_test")
                            .set_value_serialization_schema(serialization_schema_hotel)
                            .build()
                    )
                    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .set_property("security.protocol", "SSL")
                    .set_property("ssl.truststore.type", "JKS")
                    .set_property("ssl.truststore.location", "/flink/usrlib/client.truststore.jks")
                    .set_property("ssl.truststore.password", known_args.ssl_truststore_password)
                    .set_property("ssl.keystore.type", "PKCS12")
                    .set_property("ssl.keystore.location", "/flink/usrlib/client.keystore.p12")
                    .set_property("ssl.keystore.password", known_args.ssl_keystore_password)
                    .build()
        )
    )

    stream_execution_environment.execute()
