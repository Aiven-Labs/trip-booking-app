import argparse
import sys

from pyflink.common.typeinfo import Types # type: ignore
from pyflink.common.types import Row # type: ignore
from pyflink.common.watermark_strategy import WatermarkStrategy  # type: ignore
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode # type: ignore
from pyflink.datastream.connectors import DeliveryGuarantee # type: ignore
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaSource, KafkaOffsetsInitializer, KafkaRecordSerializationSchema # type: ignore
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema # type: ignore
from pyflink.datastream.functions import KeyedCoProcessFunction, MapFunction, RuntimeContext # type: ignore
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor # type: ignore
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

type_info_search_request = (
    Types
        .ROW_NAMED(
            [
                "guests",
                "customer_id",
                "location",
                "check_in_date",
                "check_out_date"
            ],
            [
                Types.INT(),
                Types.STRING(),
                Types.STRING(),
                Types.SQL_DATE(),
                Types.SQL_DATE()
            ]
        )
)

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

type_info_customer_profile = (
    Types
        .ROW_NAMED(
            [
                "customer_age",
                "customer_id",
                "customer_gender",
                "customer_home",
                "customer_hotel_loyalty",
                "customer_flight_loyalty"
            ],
            [
                Types.INT(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING()
            ]
        )
)

type_info_customer_history = (
    Types
        .ROW_NAMED(
            [
                "preferred_travel_type",
                "preferred_activities",
                "preferred_perks"
            ],
            [
                Types.STRING(),
                Types.STRING(),
                Types.STRING()
            ]
        )
)

type_info_search_request_user_activity = (
    Types
        .ROW_NAMED(
            [
                "trip_details",
                "session_data",
                "trip_type",
                "customer_profile",
                "customer_history"  
            ],
            [
                type_info_search_request,
                Types.STRING(),
                Types.STRING(),
                type_info_customer_profile,
                type_info_customer_history
            ]
        )
)

type_info_search_request_user_activity_hotels = (
    Types
        .ROW_NAMED(
            [
                "search_request_user_activity",
                "hotels"
            ],
            [
                type_info_search_request_user_activity,
                Types.OBJECT_ARRAY(Types.MAP(Types.STRING(), Types.STRING()))
            ]
        )
)

type_info_hotels_itinerary = (
    Types
        .ROW_NAMED(
            [
                "hotels",
                "itinerary"
            ],
            [
                Types.OBJECT_ARRAY(Types.MAP(Types.STRING(), Types.STRING())),
                Types.STRING()
            ]
        )
)

deserialization_schema_search_request = (
    JsonRowDeserializationSchema
        .builder()
        .type_info(type_info_search_request)
        .build()
)

deserialization_schema_user_activity = (
    JsonRowDeserializationSchema
        .builder()
        .type_info(type_info_user_activity)
        .build()
)

serialization_schema_search_request_user_activity = (
    JsonRowSerializationSchema
        .builder()
        .with_type_info(type_info_search_request_user_activity)
        .build()
)

serialization_schema_search_request_user_activity_hotels = (
    JsonRowSerializationSchema
        .builder()
        .with_type_info(type_info_search_request_user_activity_hotels)
        .build()
)

serialization_schema_hotels_itinerary = (
    JsonRowSerializationSchema
        .builder()
        .with_type_info(type_info_hotels_itinerary)
        .build()
)

class HotelsItinerary(MapFunction):

    def map(self, search_request_user_activity_hotels):
        return Row(
            hotels=search_request_user_activity_hotels.hotels,
            itinerary=" Trip Itinerary for Business Trip to Austin, Texas: Day 1: - Check in at the suggested hotel: [insert hotel name here] in Austin, Texas - Evening activity: Attend a sports competition (based on customer's preferred activity) at [insert sports venue here] - Dinner at a popular local restaurant Day 2: - Morning: Free time or work-related meetings - Afternoon: Free time or work-related meetings - Evening activity: Relax at the hotel pool (based on customer's preferred perk) Day 3: - Morning: Free time or work-related meetings - Afternoon: Free time or work-related meetings - Evening activity: Enjoy a workout at the hotel gym (based on customer's preferred perk) Day 4: - Morning: Free time or work-related meetings - Afternoon: Free time or work-related meetings - Evening activity: Visit a local brewery or winery for a networking event Day 5: - Check out of the hotel - Free time or work-related meetings until departure "
        )

class SearchRequestUserActivityHotels(KeyedCoProcessFunction):

    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        self.state = (
            runtime_context
                .get_list_state(
                    ListStateDescriptor(
                        "hotels",
                        type_info_hotel
                    )
                )
        )

    def process_element1(self, search_request_user_activity, ctx: 'KeyedCoProcessFunction.Context'):
        # retrieve the current count
        state_values = self.state.get()
        hotels = []
        for hotel in state_values:
            hotels.append({
                "Location": hotel.location,
                "Hotel": hotel.hotel,
                "Price": hotel.price,
                "Room_Size": hotel.room_size
            })
        yield Row(
            search_request_user_activity=search_request_user_activity,
            hotels=hotels
        )

    def process_element2(self, hotel, ctx: 'KeyedCoProcessFunction.Context'):
        # write the state
        self.state.add(hotel)

class SearchRequestUserActivity(KeyedCoProcessFunction):

    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        self.state = (
            runtime_context
                .get_state(
                    ValueStateDescriptor(
                        "hotel",
                        Types.STRING()
                    )
                )
        )

    def process_element1(self, search_request, ctx: 'KeyedCoProcessFunction.Context'):
        # retrieve the current count
        hotel_viewed = self.state.value()
        yield Row(
            trip_details=search_request,
            session_data=hotel_viewed,
            trip_type=get_trip_type(search_request.guests),
            customer_profile=get_customer_profile(search_request.customer_id),
            customer_history=get_customer_history(search_request.customer_id)
        )

    def process_element2(self, user_activity, ctx: 'KeyedCoProcessFunction.Context'):
        # write the state
        self.state.update(user_activity.hotel_viewed)

def get_trip_type(guest_count):
    if guest_count == 1:
        return "BUSINESS"
    else:
        return "FUN"

def get_customer_profile(customer_id):
    return (
        Row(
            customer_id=customer_id,
            customer_age=40,
            customer_gender="Male",
            customer_home="NY",
            customer_hotel_loyalty="Marriott",
            customer_flight_loyalty="Delta"
        )
    )

def get_customer_history(customer_id):
    return (
        Row(
            preferred_travel_type="business travel",
            preferred_activities="Sports competitions",
            preferred_perks="Pools, Gyms"
        )
    )

def main(known_args):
    # Gets execution environment
    stream_execution_environment = StreamExecutionEnvironment.get_execution_environment()
    # Sets execution mod
    stream_execution_environment.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # Sets parallelism
    stream_execution_environment.set_parallelism(1)
    # Specify python requirements
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
    hotels = (
        stream_table_environment
            .to_data_stream(
                stream_table_environment
                    .sql_query(
                        """
                        SELECT hotel, location, room_size, CAST(price AS STRING) AS price
                        FROM hotels
                        """
                    )
            )
    )
    search_requests = (
        get_datastream_from_kafka(
            stream_execution_environment=stream_execution_environment,
            known_args=known_args,
            topic_name="search_requests",
            deserialization_schema=deserialization_schema_search_request
        )
    )
    user_activities = (
        get_datastream_from_kafka(
            stream_execution_environment=stream_execution_environment,
            known_args=known_args,
            topic_name="user_activity",
            deserialization_schema=deserialization_schema_user_activity
        )
    )
    search_request_user_activity = (
        search_requests
            .key_by(lambda search_request: search_request.customer_id)
            .connect(user_activities.key_by(lambda user_activity: str(user_activity.customer_id)))
            .process(SearchRequestUserActivity(), output_type=type_info_search_request_user_activity)
    )
    search_request_user_activity_hotels = (
        search_request_user_activity
            .key_by(lambda search_request_user_activity: search_request_user_activity.trip_details.location)
            .connect(hotels.key_by(lambda hotel: hotel.location))
            .process(SearchRequestUserActivityHotels(), output_type=type_info_search_request_user_activity_hotels)
    )
    (
        search_request_user_activity_hotels
            .map(HotelsItinerary(), output_type=type_info_hotels_itinerary)
            .sink_to(
                KafkaSink
                    .builder()
                    .set_bootstrap_servers(known_args.bootstrap_servers)
                    .set_record_serializer(
                        KafkaRecordSerializationSchema
                            .builder()
                            .set_topic("search_responses")
                            .set_value_serialization_schema(serialization_schema_hotels_itinerary)
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

def get_datastream_from_kafka(stream_execution_environment, known_args, topic_name, deserialization_schema):
    return (
        stream_execution_environment
            .from_source(
                get_source_kafka(
                    known_args=known_args,
                    topic_name=topic_name,
                    deserialization_schema=deserialization_schema
                ),
                WatermarkStrategy.no_watermarks(),
                topic_name
            )
    )

def get_source_postgres(known_args, table_name):
    # Defines connectivity
    return (
        """
        CREATE TABLE {table_name} (
            hotel VARCHAR,
            location VARCHAR,
            room_size VARCHAR,
            price DECIMAL
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

def get_source_kafka(known_args, topic_name, deserialization_schema):
    # Builds source
    return (
        KafkaSource
            .builder()
            .set_bootstrap_servers(known_args.bootstrap_servers)
            .set_topics(topic_name)
            .set_group_id("ververica")
            .set_starting_offsets(KafkaOffsetsInitializer.latest())
            .set_value_only_deserializer(deserialization_schema)
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
