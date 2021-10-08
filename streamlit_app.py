import psycopg2
import sshtunnel
import streamlit as st
import pandas as pd

sshtunnel.SSH_TIMEOUT = 5.0
sshtunnel.TUNNEL_TIMEOUT = 5.0

try:
    with sshtunnel.SSHTunnelForwarder(
        ('ssh.pythonanywhere.com'),
        ssh_username=st.secrets["PA_USER"], ssh_password=st.secrets["PA_PASS"],
        remote_bind_address=(st.secrets["DB_HOST"], st.secrets["DB_PORT"])) as tunnel:
        #st.write('ssh connected')
        connection = psycopg2.connect(
            user=st.secrets["DB_USER"], password=st.secrets["DB_PASS"],
            host='127.0.0.1', port=tunnel.local_bind_port,
            database=st.secrets["DB_NAME"],
        )
        #@st.cache(allow_output_mutation=True, hash_funcs={"_thread.RLock": lambda _: None})
        st.title('Wind & Wind Power Generation Monitor')

        facttable = pd.read_sql("select * from  \"facttable\"", connection)
        energy = pd.read_sql("select * from \"energydata\"", connection)
        forecast = pd.read_sql("select * from \"forecastdata\"", connection)

        def to_datetime(df):

            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            return df
    
        energy = to_datetime(energy)
        forecast = to_datetime(forecast)

        st.subheader('Locations Map')
        st.map(facttable)


        if st.checkbox('Show raw data'):
            st.write('Lookup Table')
            st.dataframe(facttable)
            st.write('Forecast')
            st.dataframe(forecast)
            st.write('Energydata')
            st.dataframe(energy)

        st.subheader('Filter Forecast Time')

        hour_to_filter = st.slider('hour', 0, 23, 17)
        filtered_hour = forecast[(forecast['timestamp'].dt.hour == hour_to_filter)]
        st.subheader('Wind forecast at %s:00' % hour_to_filter)
        st.dataframe(filtered_hour)    

        connection.close()
except:
    st.write('ssh connection failed')