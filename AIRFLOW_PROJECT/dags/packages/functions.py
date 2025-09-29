#import libraries

import requests
from retry_requests import retry
import pandas as pd
import logging
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from pandas.api.types import is_numeric_dtype, CategoricalDtype
from airflow.hooks.base import BaseHook


#extract credentials saved on airflow via the ui >> admin >> connection tab

conn = BaseHook.get_connection("weather_data_id")
DB_USER = conn.login
DB_PASS = conn.password
DB_HOST = conn.host
DB_PORT = conn.port
DB_NAME = conn.schema


#constants

API_URL = "https://api.open-meteo.com/v1/forecast"
PARAMS = {
    "latitude": 6.5244,   
    "longitude": 3.3792,
    "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
    "timezone": "Africa/Lagos"
}

DB_CONFIG = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASS,
    "host": DB_HOST,
    "port": DB_PORT
}

TABLE_NAME = "daily_weather"

#enable logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


session = requests.Session()
retry_session = retry(session, retries=5, backoff_factor=0.2)


def extract_weather_data():
    try:
        logging.info("Extracting weather data from API...")
        response = retry_session.get(API_URL, params=PARAMS, timeout=30)
        response.raise_for_status()
        logging.info("Data extraction successful")
        raw_data = response.json()
        return raw_data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error extracting weather data: {e}")
        return None


def transform_weather_data(raw_data):
    try:
        logging.info("Transforming raw data...")
        daily_data = raw_data.get("daily", {})
        df = pd.DataFrame(daily_data)

        if df.empty:
            logging.warning("Transformed dataframe is empty!")
            return None
        
        #handle column formatting
        df["time"] = pd.to_datetime(df["time"]).dt.date
        df["city"] = "Lagos"
        df["temperature_2m_max"] = df["temperature_2m_max"].astype(float)
        df["temperature_2m_min"] = df["temperature_2m_min"].astype(float)
        df["precipitation_sum"] = df["precipitation_sum"].astype(float)

        
        #feature engineering
        df["day_of_week"] = pd.to_datetime(df["time"]).dt.day_name()
        df["month"] = pd.to_datetime(df["time"]).dt.month

        df["temperature_range"] = df["temperature_2m_max"] - df["temperature_2m_min"]

        df["rain_category"] = pd.cut(df["precipitation_sum"],bins=[-0.1, 0, 5, 20, float("inf")],
                                labels=["No rain", "Light", "Moderate", "Heavy"])
        df["temp7d_avg"] = df["temperature_2m_max"].rolling(window=7).mean()

        for col in df.columns:
            if is_numeric_dtype(df[col]):  
                df[col] = df[col].fillna(0)
            elif isinstance(df[col].dtype, CategoricalDtype):  
                df[col] = df[col].cat.add_categories(["unknown"]).fillna("unknown")
            else:
                df[col] = df[col].fillna("unknown")

        logging.info("Transformation complete")
        return df
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        return pd.DataFrame()

    

def load_weather_data(df):
    df = pd.DataFrame(df)
    if df.empty:
        logging.warning("Skipping load: no data to save")
        return

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            time DATE NOT NULL,
            temperature_2m_max FLOAT,
            temperature_2m_min FLOAT,
            precipitation_sum FLOAT,
            city TEXT,
            day_of_week TEXT,
            month INT,
            temperature_range FLOAT,
            rain_category TEXT,
            temp7d_avg FLOAT,
            PRIMARY KEY (time, city)
        );
        """
        cur.execute(create_table_query)

        insert_query = f"""
        INSERT INTO {TABLE_NAME} (time, temperature_2m_max, temperature_2m_min, precipitation_sum, city,
        day_of_week, month, temperature_range, rain_category, temp7d_avg)
        VALUES %s
        ON CONFLICT (time, city) DO UPDATE SET
            temperature_2m_max = EXCLUDED.temperature_2m_max,
            temperature_2m_min = EXCLUDED.temperature_2m_min,
            precipitation_sum = EXCLUDED.precipitation_sum,
            day_of_week = EXCLUDED.day_of_week,
            month = EXCLUDED.month,
            temperature_range = EXCLUDED.temperature_range,
            rain_category = EXCLUDED.rain_category,
            temp7d_avg = EXCLUDED.temp7d_avg;
        """
        values = [
            (row["time"], row["temperature_2m_max"], row["temperature_2m_min"], 
            row["precipitation_sum"], row["city"],  row["day_of_week"], row["month"], 
            row["temperature_range"], row["rain_category"], row["temp7d_avg"])
            for _, row in df.iterrows()
        ]
        execute_values(cur, insert_query, values)
        conn.commit()
        cur.close()
        conn.close()

        logging.info("Weather data successfully upserted into database")
    except Exception as e:
        logging.error(f"Error loading data into database: {e}")
        raise   
    finally:
        if conn:
            conn.close()

