import psycopg2
import sshtunnel
import streamlit as st

sshtunnel.SSH_TIMEOUT = 5.0
sshtunnel.TUNNEL_TIMEOUT = 5.0

try:
    with sshtunnel.SSHTunnelForwarder(
        ('ssh.pythonanywhere.com'),
        ssh_username=st.secrets["PA_USER"], ssh_password=st.secrets["PA_PASS"],
        remote_bind_address=(st.secrets["DB_HOST"], st.secrets["DB_PORT"])) as tunnel:
        st.write('ssh connected')
        connection = psycopg2.connect(
            user=st.secrets["DB_USER"], password=st.secrets["DB_PASS"],
            host='127.0.0.1', port=tunnel.local_bind_port,
            database=st.secrets["DB_NAME"],
        )
        #@st.cache(allow_output_mutation=True, hash_funcs={"_thread.RLock": lambda _: None})
        st.write('connection initiated')

        #@st.cache(ttl=600)
        def run_query(query):
            with connection.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()

        rows = run_query("SELECT * FROM energydata;")

        # Print results.
        for row in rows:
            st.write(row)
            #st.write('ssh connection established')
        connection.close()
except:
    st.write('ssh connection failed')
