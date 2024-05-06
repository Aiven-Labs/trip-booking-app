import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import pandas as pd
from decimal import Decimal

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
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
)
col1, col2, col3 = st.columns(3)

def submit_data(form_payload):
    producer.send(SEARCH_REQUESTS_TOPIC, str(form_payload).encode('utf-8'))

def send_clickstream_data(clickstream_event):
    print(clickstream_event)
    producer.send(CLICKSTREAM_TOPIC, str(clickstream_event).encode('utf-8'))

col1, col2, col3 = st.columns(3)
with col1:
    with st.form("Hotel Search Form", clear_on_submit=False):
        st.write("Plan your next trip")
        customer_id = "1"
        location_val = st.selectbox('Location', ['Austin, Texas', 'Nashville, Tennessee', 'Atlanta, Georgia', "Los Angeles, California"])
        check_in_date_val = st.date_input("When are you checking in?", datetime.date(2024, 7, 6))
        check_out_date_val = st.date_input("When are you checking out?", datetime.date(2024, 7, 10))
        guests_val = st.number_input("How many guests?", value=None, placeholder="Type a number...")
        # Every form must have a submit button.
        submitted = st.form_submit_button("Submit")
        if submitted:
            submit_data({
                "customer_id": customer_id,
                "location": location_val,
                "check_in_date": check_in_date_val,
                "check_out_date": check_out_date_val,
                "guests": guests_val
            })
            with st.spinner("Generating results..."):
                consumer = KafkaConsumer(
                        SEARCH_RESPONSES_TOPIC,
                        bootstrap_servers=os.environ["KAFKA_URL"],
                        client_id = "CONSUMER_CLIENT_ID",
                        group_id = "CONSUMER_GROUP_ID",
                        security_protocol="SSL",
                        ssl_cafile="ca.pem",    
                        ssl_certfile="service.cert",
                        ssl_keyfile="service.key",
                    )
                while True:
                    for search_response in consumer.poll().values():
                        search_response = search_response[0].value.decode('utf-8')
                        print(search_response)
                        with col2:
                            search_response = eval(search_response)
                            for hotel in search_response["hotels"]:
                                container = st.container(border=True)
                                container.write(f"<a href='#' id='my-link'>{hotel["Hotel"]}</a>", unsafe_allow_html=True)
                                container.write(f"${hotel["Price"]} per night")
                                container.write(f"{hotel["Room_Size"]}")
                        with col3:
                            with st.popover("Provide feedback for your itinerary"):
                                with st.form("feedback_form", clear_on_submit=False):
                                    st.write("Did you like your generated itinerary?")
                                    st.checkbox("Yes", key="Yes")
                                    st.checkbox("No", key="No")
                                    st.multiselect(
                                    "What did you like?",
                                    ["Hotel", "Restaurants", "Activities"], key="Positive")
                                    st.multiselect(
                                    "What could we have improved?",
                                    ["Hotel", "Restaurants", "Activities"], key="Negative")
                                    st.form_submit_button()
                            st.write(search_response["itinerary"])