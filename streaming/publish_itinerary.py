import argparse
import requests # type: ignore
import datetime
import sys

from pyflink.common.typeinfo import Types # type: ignore
from pyflink.common.types import Row # type: ignore
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode # type: ignore
from pyflink.datastream.connectors import DeliveryGuarantee # type: ignore
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema # type: ignore
from pyflink.datastream.formats.json import JsonRowSerializationSchema # type: ignore
from pyflink.datastream.functions import MapFunction # type: ignore

api_url = "https://api.openai.com/v1/chat/completions"

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

serialization_schema_hotels_itinerary = (
    JsonRowSerializationSchema
        .builder()
        .with_type_info(type_info_hotels_itinerary)
        .build()
)

class Itinerary(MapFunction):

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
                            You should emphasize trip activities based on the customer\'s preferred activities.
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
    (
        stream_execution_environment
            .from_collection(
                collection=[
                    Row(
                        search_request_user_activity=Row(
                            customer_history=Row(
                                preferred_activities="Sports competitions",
                                preferred_perks="Pools, Gyms",
                                preferred_travel_type="business travel"
                            ),
                            customer_profile=Row(
                                customer_age=40,
                                customer_flight_loyalty="Delta",
                                customer_gender="Male",
                                customer_home="NY",
                                customer_hotel_loyalty="Marriott",
                                customer_id="1"
                            ),
                            session_data="Courtyard Marriott Austin",
                            trip_type="BUSINESS",
                            trip_details=Row(
                                check_in_date=datetime.date(2024, 7, 6),
                                check_out_date=datetime.date(2024, 7, 10),
                                customer_id="1",
                                guests=1,
                                location="Austin, Texas"
                            )
                        ),
                        hotels=[
                            {
                                "Hotel": "Hotel A",
                                "Location": "Austin, Texas",
                                "Price": "150",
                                "Room_Size": "Standard"
                            },
                            {
                                "Hotel": "Hotel B",
                                "Location": "Austin, Texas",
                                "Price": "200",
                                "Room_Size": "Deluxe"
                            },
                            {
                                "Hotel": "Hotel C",
                                "Location": "Austin, Texas",
                                "Price": "120",
                                "Room_Size": "Standard"
                            },
                            {
                                "Hotel": "Hotel D",
                                "Location": "Austin, Texas",
                                "Price": "80",
                                "Room_Size": "Deluxe"
                            }
                        ]
                    )
                ],
                type_info=type_info_search_request_user_activity_hotels
            )
            .map(
                Itinerary(known_args=known_args),
                output_type=type_info_hotels_itinerary
            )
            .sink_to(
                KafkaSink
                    .builder()
                    .set_bootstrap_servers(known_args.bootstrap_servers)
                    .set_record_serializer(
                        KafkaRecordSerializationSchema
                        .builder()
                        .set_topic("search_responses_test")
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

    stream_execution_environment.execute()
