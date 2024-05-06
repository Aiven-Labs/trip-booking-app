import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import pandas as pd

import datetime
import json
from kafka import KafkaProducer, KafkaConsumer

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
    bootstrap_servers=f"priceline-ai-demo-rmehta-demo.i.aivencloud.com:18618",
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
)
def protected():
    if __name__ == '__main__':
        def submit_data(form_payload):
            producer.send(SEARCH_REQUESTS_TOPIC, str(form_payload).encode('utf-8'))

        def send_clickstream_data(clickstream_event):
            print(clickstream_event)
            producer.send(CLICKSTREAM_TOPIC, str(clickstream_event).encode('utf-8'))

        def consume_kafka_messages():
            consumer = KafkaConsumer(
                SEARCH_RESPONSES_TOPIC,
                bootstrap_servers=f"priceline-ai-demo-rmehta-demo.i.aivencloud.com:18618",
                client_id = "CONSUMER_CLIENT_ID",
                group_id = "CONSUMER_GROUP_ID",
                security_protocol="SSL",
                ssl_cafile="ca.pem",
                ssl_certfile="service.cert",
                ssl_keyfile="service.key",
                auto_offset_reset='latest',  # Set to 'latest' to read from the latest offset
                enable_auto_commit= True  # Enable auto commit to automatically commit offsets
            )
            print(consumer)
            print(len(consumer))
            
                
        hotels = ["Courtyard Marriott Austin", "Aloft Austin", "Hyatt Austin", "Holiday Inn Express Austin"]
        st.title("Travel Booking App")
        st.write(f"Welcome back {st.session_state.name}. We're happy to see you again!")
        profile, browsing, search = st.tabs(["Your Profile", "Hotel Browsing", "Hotel Search"])
        with profile:
            st.write(f"**Name**: {st.session_state.name}")
            st.write("**Preferred Hotel Loyalty**: Marriott")
            st.write("**Preferred Airline Loyalty**: Delta")
            st.write("**Preferred Activities**: Sports Competitions")
            st.write("**Preferred Hotel Amenities**: Pool, Gym, Coffee Shop")
            st.write("**Past Trips**")
            df = pd.DataFrame({'Date': ["March 3rd, 2024", "March 17th, 2024"], 'Destination': ["Los Angeles", "Austin"], 'Hotel': ["Courtyard Marriott Los Angeles", "Aloft Austin"]})
            st.dataframe(df, hide_index=True)
        with browsing:
            for i in hotels:
                if st.button(i):
                    clickstream_event = {
                        "customer_id": 1,
                        "hotel_viewed": i
                    }
                    send_clickstream_data(clickstream_event)
        with search:
            col1, col2, col3 = st.columns(3)
            consumer = KafkaConsumer(
                SEARCH_RESPONSES_TOPIC,
                bootstrap_servers=f"priceline-ai-demo-rmehta-demo.i.aivencloud.com:18618",
                client_id = "CONSUMER_CLIENT_ID",
                group_id = "CONSUMER_GROUP_ID",
                security_protocol="SSL",
                ssl_cafile="ca.pem",
                ssl_certfile="service.cert",
                ssl_keyfile="service.key",
                auto_offset_reset='latest',  # Set to 'latest' to read from the latest offset
                enable_auto_commit= True  # Enable auto commit to automatically commit offsets
            )
            with col1:
                with st.form("Hotel Search Form", clear_on_submit=False):
                    st.write("Plan your next trip")
                    customer_id = "1"
                    location_val = st.selectbox('Location', ['Austin', 'Nashville', 'Atlanta', "Los Angeles"])
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
                            while True:
                                for search_response in consumer.poll().values():
                                    search_response = search_response.value.decode('utf-8')
                                    print(search_response)
                                    with col2:
                                        container = st.container(border=True)
                                        container.write("<a href='#' id='my-link'>Courtyard Marriott Austin</a>", unsafe_allow_html=True)
                                        container.write("$199.99 per night")
                                        container.write("1 King")
                                        container2 = st.container(border=True)
                                        container2.write("<a href='#' id='my-link'>Aloft Austin</a>", unsafe_allow_html=True)
                                        container2.write("$199.99 per night")
                                        container2.write("1 King")
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
                                        st.write(search_response)
if st.session_state["authentication_status"]:
    authenticator.logout()
    protected()
elif st.session_state["authentication_status"] is False:
    st.error('Username/password is incorrect')
elif st.session_state["authentication_status"] is None:
    st.warning('Please enter your username and password')