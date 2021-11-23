import csv
import os
import sys
import psycopg2
import psycopg2.extras

def write2database(inputFile1, inputFile2):

    DB_HOST = os.environ['DB_HOST']
    DB_NAME = os.environ['DB_NAME']
    DB_USER = os.environ['DB_USER']
    DB_PASS = os.environ['DB_PASS']
    DB_PORT = os.environ['DB_PORT']

    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
        print('Connection succesful')
    except:
        print('Connection failed')

    cursor = conn.cursor()

    try:
        cursor.execute("""
        CREATE TABLE if not exists forecastdata (
            "timestamp" integer,
            "regionID" integer NOT NULL,
            "temperature_C" real,
            "wind_speed_m/s" real,
            "wind_direction" real,
            "wind_gust" real,
            "cloudCover_pct" real,
            "weathercode" integer
        );
        
        """)
        print('Table created')
    except:
        print('Table not created')

    with open(inputFile1, 'r') as f:
        next(f)
        cursor.copy_from(f, 'facttable', sep=',')
    
    with open(inputFile2, 'r') as f:
        next(f)
        cursor.copy_from(f, 'forecastdata', sep=',')

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    INPUT_FILE1 = sys.argv[1] # this is the csv file to move to PostgreSQL Database
    INPUT_FILE2 = sys.argv[2]
    try:
        write2database(INPUT_FILE1)
        write2database(INPUT_FILE2)
    except:
        print("Error writing csv to database")
