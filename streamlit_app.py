import psycopg2
import sshtunnel
import streamlit as st
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from datetime import date

sshtunnel.SSH_TIMEOUT = 20.0
sshtunnel.TUNNEL_TIMEOUT = 20.0

try:
    with sshtunnel.SSHTunnelForwarder(
        ('ssh.pythonanywhere.com'),
        ssh_username=st.secrets["PA_USER"], ssh_password=st.secrets["PA_PASS"],
        remote_bind_address=(st.secrets["DB_HOST"], st.secrets["DB_PORT"])) as tunnel:

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
        '''convert unix time to datetime'''
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

    def to_region(df):
        '''extract unique regions'''
        regions = df['region'].unique()
        return regions

    regions = to_region(facttable)

    st.subheader('Houerly Wind Power Generation per Region')
    
    # create region drop down
    region_to_filter = st.selectbox('Select region:', regions)

    # join energy & facttable
    energyfacts = pd.merge(energy, facttable, on='regionID', how='left')

    # join forecast & facttable
    forecastfacts = pd.merge(forecast, facttable, on='regionID', how='left')

    # select data to plot for chosen region from region drop down
    energy_per_id  = energyfacts.loc[energyfacts['region']==region_to_filter]
    timestamp = energy_per_id['timestamp']

    # plot history of wind power generation over time
    fig, ax = plt.subplots()

    ax.plot(
        timestamp,
        energy_per_id["windPower_kWh"]
    )

    ax.set_xlabel("time")
    ax.set_ylabel("wind power in kWh")

    ax.xaxis.set_ticks([min(timestamp), max(timestamp)])

    st.pyplot(fig)

    #today = date.today()
    #filtered_hour = forecastfacts[(forecastfacts['timestamp'].dt.date == today) & (forecastfacts['region'] == region_to_filter)]
  
    st.subheader ('Wind Power Generation over Wind Speed for %s' %  (region_to_filter))

    # plot wind power generation over wind speed for possible future regression task
    fig, ax = plt.subplots()

    ax.scatter(
        energy_per_id['windSpeed_m/s'],
        energy_per_id["windPower_kWh"]
    )

    ax.set_xlabel("wind speed in m/s")
    ax.set_ylabel("wind power in kWh")

    st.pyplot(fig)

    connection.close()
except:
<<<<<<< HEAD
    st.write('timeout')
=======
    st.write('timeout')
>>>>>>> d83d422eb187a250de17003b20bb94401bdf5f66
