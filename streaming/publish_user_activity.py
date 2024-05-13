import argparse
import sys

from pyflink.common.typeinfo import Types  # type: ignore
from pyflink.common.types import Row  # type: ignore
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode  # type: ignore
from pyflink.datastream.connectors import DeliveryGuarantee  # type: ignore
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema  # type: ignore
from pyflink.datastream.formats.json import JsonRowSerializationSchema  # type: ignore

type_info_user_activity = (
    Types
        .ROW_NAMED(
            [
                "customer_id",
                "hotel_viewed"
            ],
            [
                Types.INT(),
                Types.STRING()
            ]
        )
)

serialization_schema_user_activity = (
    JsonRowSerializationSchema
        .builder()
        .with_type_info(type_info_user_activity)
        .build()
)


def add_arguments(parser):
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
    (
        stream_execution_environment
            .from_collection(
                collection=[
                    Row(
                        customer_id=1,
                        hotel_viewed="Courtyard Marriott Austin"
                    )
                ],
                type_info=type_info_user_activity
            )
            .sink_to(
                KafkaSink
                    .builder()
                    .set_bootstrap_servers(known_args.bootstrap_servers)
                    .set_record_serializer(
                        KafkaRecordSerializationSchema
                            .builder()
                            .set_topic("user_activity_test")
                            .set_value_serialization_schema(serialization_schema_user_activity)
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
