import psycopg2
import sshtunnel
import streamlit

sshtunnel.SSH_TIMEOUT = 5.0
sshtunnel.TUNNEL_TIMEOUT = 5.0

try:
    with sshtunnel.SSHTunnelForwarder(
        ('ssh.pythonanywhere.com'),
        ssh_username=PA_USER, ssh_password=PA_PASS,
        remote_bind_address=(DB_HOST, DB_PORT)) as tunnel:
        connection = psycopg2.connect(
            user=DB_USER, password=DB_PASS,
            host='127.0.0.1', port=tunnel.local_bind_port,
            database=DB_NAME,
        )
        print('ssh connection established')
        connection.close()
except:
    print('ssh connection failed')