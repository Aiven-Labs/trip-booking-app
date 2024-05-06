import streamlit as st
import pandas as pd
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
st.set_page_config(
    page_title="Customer Profile",
    page_icon="👋",
    layout="wide"
)

with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
)
authenticator.login()

def protected():
    st.header("Customer Profile")
    st.write(f"**Name**: {st.session_state.name}")
    st.write("**Preferred Hotel Loyalty**: Marriott")
    st.write("**Preferred Airline Loyalty**: Delta")
    st.write("**Preferred Activities**: Sports Competitions")
    st.write("**Preferred Hotel Amenities**: Pool, Gym, Coffee Shop")
    st.write("**Past Trips**")
    df = pd.DataFrame({'Date': ["March 3rd, 2024", "March 17th, 2024"], 'Destination': ["Los Angeles", "Austin"], 'Hotel': ["Courtyard Marriott Los Angeles", "Aloft Austin"]})
    st.dataframe(df, hide_index=True)
if st.session_state["authentication_status"]:
    authenticator.logout()
    protected()
elif st.session_state["authentication_status"] is False:
    st.error('Username/password is incorrect')
elif st.session_state["authentication_status"] is None:
    st.warning('Please enter your username and password')