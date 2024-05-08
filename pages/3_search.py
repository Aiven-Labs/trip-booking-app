import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import pandas as pd
from decimal import Decimal
import uuid

import datetime
import json
from kafka import KafkaProducer, KafkaConsumer
import os
from dotenv import load_dotenv
load_dotenv()

SEARCH_REQUESTS_TOPIC = "search_requests"
SEARCH_RESPONSES_TOPIC = "search_responses"
CLICKSTREAM_TOPIC = "user_activity"
with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)
st.set_page_config(layout="wide")
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
)
hotel_list_results = ""
itinerary_results = ""

authenticator.login()
producer = KafkaProducer(
    bootstrap_servers=os.environ["KAFKA_URL"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
)
col1, col2, col3 = st.columns(3)

def submit_data(form_payload):
    producer.send(SEARCH_REQUESTS_TOPIC, form_payload)

def send_clickstream_data(clickstream_event):
    producer.send(CLICKSTREAM_TOPIC, clickstream_event)

col1, col2, col3 = st.columns(3)
consumer = KafkaConsumer(
        SEARCH_RESPONSES_TOPIC,
        bootstrap_servers=os.environ["KAFKA_URL"],
        client_id = "CONSUMER_CLIENT_ID",
        group_id = "CONSUMER_GROUP_ID",
        security_protocol="SSL",
        ssl_cafile="ca.pem",    
        ssl_certfile="service.cert",
        ssl_keyfile="service.key",
        auto_offset_reset='latest',  # Set to 'latest' to read from the latest offset
        enable_auto_commit=False
    )
with col1:
    with st.form("Hotel Search Form", clear_on_submit=False):
        st.write("Plan your next trip")
        customer_id = "1"
        location_val = st.selectbox('Location', ['Austin, Texas', 'Nashville, Tennessee', 'Atlanta, Georgia', "Los Angeles, California"])
        check_in_date_val = st.date_input("When are you checking in?", datetime.date(2024, 7, 6))
        check_out_date_val = st.date_input("When are you checking out?", datetime.date(2024, 7, 10))
        guests_val = st.number_input("How many guests?", value=1, placeholder="Type a number...")
        # Every form must have a submit button.
        submitted = st.form_submit_button("Submit")
        if submitted:
            submit_data({
                "customer_id": customer_id,
                "location": location_val,
                "check_in_date": check_in_date_val.strftime("%Y-%m-%d"),
                "check_out_date": check_out_date_val.strftime("%Y-%m-%d"),
                "guests": guests_val
            })
for search_response in consumer:
    print(search_response)
    search_response = search_response.value.decode('utf-8')
    search_response = json.loads(search_response, parse_float=Decimal)
    print(search_response)
    with col2:
        print("hotels", search_response["hotels"])
        hotels = search_response["hotels"]
        for hotel in search_response["hotels"]:
            container = st.container(border=True)
            container.write(f"<a href='#' id='my-link'>{hotel["Hotel"]}</a>", unsafe_allow_html=True)
            container.write(f"${hotel["Price"]} per night")
            container.write(f"{hotel["Room_Size"]}")
    with col3:
        with st.popover("Provide feedback for your itinerary"):
            random_uuid = uuid.uuid4()
            feedback_uuid = str(random_uuid)
            with st.form(f"feedback_form_{feedback_uuid}", clear_on_submit=False):
                st.write("Did you like your generated itinerary?")
                likes_itinerary = st.checkbox(f"Yes_{feedback_uuid}", key=f"Yes_{feedback_uuid}")
                dislikes_itinerary = st.checkbox(f"No_{feedback_uuid}", key=f"No_{feedback_uuid}")
                feedback_likes = st.multiselect(
                "What did you like?",
                ["Hotel", "Restaurants", "Activities"], key=f"Positive_{feedback_uuid}")
                feedback_dislikes = st.multiselect(
                f"What could we have improved?_{feedback_uuid}",
                ["Hotel", "Restaurants", "Activities"], key=f"Negative_{feedback_uuid}")
                feedback_submitted = st.form_submit_button()
                if feedback_submitted:
                    send_clickstream_data({
                        "likes_itinerary": likes_itinerary,
                        "dislikes_itinerary": dislikes_itinerary,
                        "feedback_likes": feedback_likes,
                        "feedback_dislikes": feedback_dislikes
                    })
        st.write(search_response["itinerary"])
    consumer.commit()
    break