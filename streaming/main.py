import argparse
import requests # type: ignore
import sys

from pyflink.common.typeinfo import Types # type: ignore
from pyflink.common.types import Row, RowKind # type: ignore
from pyflink.common.watermark_strategy import WatermarkStrategy  # type: ignore
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode # type: ignore
from pyflink.datastream.connectors import DeliveryGuarantee # type: ignore
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaSource, KafkaOffsetsInitializer, KafkaRecordSerializationSchema # type: ignore
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema # type: ignore
from pyflink.datastream.functions import FilterFunction, KeyedCoProcessFunction, MapFunction, RuntimeContext # type: ignore
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor # type: ignore
from pyflink.table import StreamTableEnvironment # type: ignore

api_url = "https://api.openai.com/v1/chat/completions"

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

class IsNotUpdateBefore(FilterFunction):

    def filter(self, value):
        return value.get_row_kind() is not RowKind.UPDATE_BEFORE

class HotelsItinerary(MapFunction):

    def __init__(self, known_args):
        self.open_api_key = known_args.open_api_key

    def map(self, search_request_user_activity_hotels):
        payload = {
            "model": "gpt-3.5-turbo",
            "temperature": 0.7,
            "messages": [{
                "role": "user",
                "content": f"""You are a travel agent. DO NOT include the customer profile in your output. 
                            Based on the trip_type information, this is a {search_request_user_activity_hotels.search_request_user_activity.trip_type} trip.
                            DO NOT make any mentions of the customer's identity in your itinerary.
                            YOU SHOULD suggest this hotel: {search_request_user_activity_hotels.search_request_user_activity.session_data}, because the customer is interested in those hotels.
                            If it is a business trip, you SHOULD ONLY suggest activities during the evenings, not during the MORNING OR AFTERNOON. 
                            IF IT IS A FUN TRIP, DO NOT SUGGEST ACTIVITIES DURING THE MORNING OR AFTERNOON IF IT IS A BUSINESS TRIP. THE USER WILL BE AT WORK.
                            Make a trip itinerary based on the following parameters.{search_request_user_activity_hotels.search_request_user_activity.as_dict(recursive=True)} 
                            You should emphasize trip activities based on the customer\'s preferred activities. Give SPECIFIC recommendations for activities based on real things to do in that city.
                            """
            }]
        }
        headers =  {
            "Authorization": f"Bearer {self.open_api_key}"
        }
        response = requests.post(api_url, json=payload, headers=headers)
        return Row(
            hotels=search_request_user_activity_hotels.hotels,
            itinerary=response.json()["choices"][0]["message"]["content"]
        )

class SearchRequestUserActivityHotels(KeyedCoProcessFunction):

    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        self.state = (
            runtime_context
                .get_map_state(
                    MapStateDescriptor(
                        "hotels",
                        Types.STRING(),
                        type_info_hotel
                    )
                )
        )

    def process_element1(self, search_request_user_activity, ctx: 'KeyedCoProcessFunction.Context'):
        # retrieve the current count
        state_values = self.state.values()
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
        if (hotel.get_row_kind() is RowKind.INSERT):
            self.state.put(hotel.hotel, hotel)
        elif (hotel.get_row_kind() is RowKind.UPDATE_AFTER):
            self.state.put(hotel.hotel, hotel)
        elif (hotel.get_row_kind() is RowKind.DELETE):
            self.state.remove(hotel.hotel)

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
    if customer_id == "1":
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
    elif customer_id == "2":
        return (
            Row(
                customer_id=customer_id,
                customer_age=21,
                customer_gender="Female",
                customer_home="LA",
                customer_hotel_loyalty="Hilton",
                customer_flight_loyalty="United"
            )
        )

def get_customer_history(customer_id):
    if customer_id == "1":
        return (
            Row(
                preferred_travel_type="Business Travel",
                preferred_activities="Sports Competitions",
                preferred_perks="Pools, Gyms"
            )
        )
    elif customer_id == "2":
        return (
            Row(
                preferred_travel_type="Leisure Travel",
                preferred_activities="Music Concerts",
                preferred_perks="Music Venues, Concert Halls"
            )
        )

def main(known_args):
    # Gets execution environment
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
            .map(HotelsItinerary(known_args=known_args), output_type=type_info_hotels_itinerary)
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
        'connector' = 'postgres-cdc',
        'hostname' = '{hostname}',
        'port' = '{port}',
        'username' = '{username}',
        'password' = '{password}',
        'database-name' = '{database_name}',
        'schema-name' = 'public',
        'table-name' = '{table_name}',
        'slot.name' = 'flink',
        'decoding.plugin.name' = 'pgoutput'
        )
        """.format(
            hostname=known_args.hostname,
            port=known_args.port,
            username=known_args.username,
            password=known_args.password,
            database_name=known_args.database_name,
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
    # Adds open api key
    (
        parser
            .add_argument(
                "--open-api-key",
                dest="open_api_key",
                required=True,
                help="Open API Key."
            )
    )
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
    main(known_args)
