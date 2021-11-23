import csv
import os
import sys
import psycopg2
import psycopg2.extras

def write2database(inputFile1):
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
    CREATE TABLE if not exists facttable (
    "regionID" integer NOT NULL,
    "region" text,
    "lat" real,
    "lon" real,
    "weatheronline_sun" integer,
    "weatheronline_key" integer
    );

    """)
        print('Table created')
    except:
        print('Table not created')

    with open(inputFile1, 'r') as f:
        next(f)
        cursor.copy_from(f, 'facttable', sep=',')

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":

    INPUT_FILE1 = sys.argv[1] # this is the csv file to move to PostgreSQL Database

    try:
        write2database(INPUT_FILE1)
    except:
        print("Error writing csv to database")