import streamlit as st
from kafka import KafkaProducer, KafkaConsumer
import os
from dotenv import load_dotenv
load_dotenv()


CLICKSTREAM_TOPIC = "user_activity"

producer = KafkaProducer(
    bootstrap_servers=os.environ["KAFKA_URL"],
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
)
def send_clickstream_data(clickstream_event):
    print(clickstream_event)
    producer.send(CLICKSTREAM_TOPIC, str(clickstream_event).encode('utf-8'))

hotels = ["Courtyard Marriott Austin", "Aloft Austin", "Hyatt Austin", "Holiday Inn Express Austin"]
for i in hotels:
    if st.button(i):
        clickstream_event = {
            "customer_id": 1,
            "hotel_viewed": i
        }
        send_clickstream_data(clickstream_event)