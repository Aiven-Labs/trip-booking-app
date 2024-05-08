from kafka import KafkaConsumer, KafkaProducer
import json
import datetime
from langchain_openai import OpenAI
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    host=os.environ["POSTGRES_HOST"],
    port=os.environ["POSTGRES_PORT"],
    database=os.environ["POSTGRES_DATABASE"]
)

SEARCH_REQUESTS_TOPIC = "search_requests"
SEARCH_RESPONSES_TOPIC = "search_responses"
CLICKSTREAM_TOPIC = "user_activity"

clickstream_history = {}

producer = KafkaProducer(
    bootstrap_servers=os.environ["KAFKA_URL"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
)

def search_response(hotels, itinerary):
    response = {
        "hotels": hotels,
        "itinerary": itinerary
    }
    producer.send(SEARCH_RESPONSES_TOPIC, response)

def retrieve_clickstream_data(customer_id):
    customer_id = str(customer_id)
    if customer_id in clickstream_history:
        return clickstream_history[customer_id][-1]
    else:
        return {}

def retrieve_customer_profile(customer_id):
    return {
        "customer_id": 1,
        "customer_age": 40,
        "customer_gender": "Male",
        "customer_home": "NY",
        "customer_hotel_loyalty": "Marriott",
        "customer_flight_loyalty": "Delta",
    }

def retrieve_customer_history(customer_id):
    return {
        "preferred_travel_type": "business travel",
        "preferred_activities": "Sports competitions",
        "preferred_perks": "Pools, Gyms"
    }

def retrieve_customer_personalization(customer_id):
    print(customer_id)

def determine_trip_type(guest_count):
    if guest_count == 1:
        return "BUSINESS"
    else:
        return "FUN"

def retrieve_hotels(location):
    cur = conn.cursor()
    sql_query = f"SELECT * FROM Hotels WHERE Location = '{location}'"
    cur.execute(sql_query)
    results = cur.fetchall()
    response = []
    print(results)
    for row in results:
        response.append({
            "Location": row[0],
            "Hotel": row[1],
            "Price": str(row[2]),
            "Room_Size": row[3]
        })
    print(results)
    return response

def request_itinerary(request_payload):
    model = OpenAI()
    print("REQUEST", request_payload["trip_type"])
    response = model.invoke(f"""You are a travel agent. DO NOT include the customer profile in your output. 
                            Based on the trip_type information, this is a {request_payload["trip_type"]} trip.
                            DO NOT make any mentions of the customer's identity in your itinerary.
                            YOU SHOULD suggest this hotel: {request_payload["session_data"]}, because the customer is interested in those hotels.
                            If it is a business trip, you SHOULD ONLY suggest activities during the evenings, not during the MORNING OR AFTERNOON. 
                            IF IT IS A FUN TRIP, DO NOT SUGGEST ACTIVITIES DURING THE MORNING OR AFTERNOON IF IT IS A BUSINESS TRIP. THE USER WILL BE AT WORK.
                            Make a trip itinerary based on the following parameters.{request_payload} 
                            You should emphasize trip activities based on the customer\'s preferred activities.
                            """)
    return response

def process_search_request(search_request):
    search_request = eval(search_request)
    #retrieve customer profile
    trip_type = determine_trip_type(search_request["guests"])
    profile = retrieve_customer_profile(search_request["customer_id"])
    #retrieve customer history
    history = retrieve_customer_history(search_request["customer_id"])
    clickstream = {}
    if str(search_request["customer_id"]) in clickstream_history:
        clickstream = retrieve_clickstream_data(str(search_request["customer_id"]))
    
    itinerary_request = {
        "trip_type": trip_type,
        "session_data": clickstream,
        "trip_details": search_request,
        "customer_profile": profile,
        "customer_history": history
    }
    hotels = retrieve_hotels(search_request["location"])
    itinerary = request_itinerary(itinerary_request)
    search_response(hotels, itinerary)
topics = [SEARCH_REQUESTS_TOPIC, CLICKSTREAM_TOPIC]
consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=os.environ["KAFKA_URL"],
    client_id = "CONSUMER_CLIENT_ID",
    group_id = "CONSUMER_GROUP_ID",
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
)

def add_to_clickstream_history(clickstream_event):
    clickstream_event = eval(clickstream_event)
    customer_id = str(clickstream_event["customer_id"])
    if customer_id not in clickstream_history:
        clickstream_history[customer_id] = [clickstream_event["hotel_viewed"]]
    else:
        clickstream_history[customer_id].append(clickstream_event["hotel_viewed"])

while True:
    for message in consumer.poll().values():
        print("Message", message)
        if message[0].topic == "user_activity":
            add_to_clickstream_history(message[0].value.decode('utf-8'))
        else:
            print(message)
            print(message[0].value.decode('utf-8'))
            process_search_request(message[0].value.decode('utf-8'))