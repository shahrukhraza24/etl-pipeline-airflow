##pip install apache-airflow pandas psycopg2 requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import psycopg2

API_KEY = "YOUR_OPENWEATHER_API_KEY"
CITY = "Sydney"
DB_CONFIG = {
    "dbname": "weather_db",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": 5432
}

def extract():
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    response = requests.get(url).json()
    data = {
        "city": CITY,
        "temperature": response["main"]["temp"],
        "humidity": response["main"]["humidity"],
        "weather": response["weather"][0]["description"],
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }
    pd.DataFrame([data]).to_csv("/tmp/weather_data.csv", index=False)

def transform():
    df = pd.read_csv("/tmp/weather_data.csv")
    df["temperature_fahrenheit"] = (df["temperature"] * 9/5) + 32
    df.to_csv("/tmp/transformed_weather_data.csv", index=False)

def load():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            city TEXT, temperature REAL, temperature_fahrenheit REAL,
            humidity INT, weather TEXT, timestamp TIMESTAMP
        )
    """)
    df = pd.read_csv("/tmp/transformed_weather_data.csv")
    for _, row in df.iterrows():
        cur.execute("INSERT INTO weather VALUES (%s, %s, %s, %s, %s, %s)",
                    (row["city"], row["temperature"], row["temperature_fahrenheit"], row["humidity"], row["weather"], row["timestamp"]))
    conn.commit()
    cur.close()
    conn.close()

dag = DAG("weather_etl", schedule_interval="@hourly", start_date=datetime(2025, 3, 9))
extract_task = PythonOperator(task_id="extract", python_callable=extract, dag=dag)
transform_task = PythonOperator(task_id="transform", python_callable=transform, dag=dag)
load_task = PythonOperator(task_id="load", python_callable=load, dag=dag)

extract_task >> transform_task >> load_task
