# Weather ETL Project

This project implements an ETL pipeline to extract, transform, and load daily weather data for Lagos into a PostgreSQL database using **Apache Airflow**.

---

## Table of Contents

- Project Overview
- Requirements
- Setup Instructions
- Running Airflow
- DAG Details
- Why This Data Source?
- Data Transformations Applied

  
---

## Project Overview

The Weather ETL pipeline performs the following steps:

1. **Extract**: Fetch daily weather data from the Open-Meteo API.
2. **Transform**: Clean and format the data using Pandas (e.g., date formatting, temperature calculations, rain categories).
3. **Load**: Insert or upsert the data into a PostgreSQL table.

The pipeline is orchestrated using **Airflow DAGs** to allow daily scheduled runs.

---

## Requirements

- Python 3.10+
- Apache Airflow 2.7.1
- PostgreSQL 16+
- `requests`, `pandas`, `psycopg2-binary`, `retry_requests`

---

## Setup Instructions

1. **Clone the repository**:

git clone https://github.com/King-Juliet/weather_data.git
cd weather_data

2. **Exectute the test ETL script to confirm everything works as expected**:
   
   - Create database on Postgres and name it weatherdb
     
   - Activate virtual environment
     
   - Execute the command below to pip install the needed libraries

     pip install -r requirements.txt

   - Execute all the cells in the test.ipynb
  
  
  ## Running airflow:
      - Start up the docker engine
          
      - Navigate to the AIRFLOW_PROJECT, then execute the command below to spin up the airflow containers
          docker-compose up --build
          
      -   Navigate to the airflow web servere UI, then create connection id (e.g. weather_data_id) for the destination database of the extracted data
          
## DAG details:

     DAG ID: weather_etl_dag

     Tasks:

       extract_task: Fetch weather data from Open-Meteo API.

       transform_task: Transform and clean the raw data.

       load_task: Insert/upsert the data into PostgreSQL.

     taskflow: extract_task -> transform_task -> load_task


## Why This Data Source?
The Open-Meteo API data source was used because it allows the business to access up-to-date and accurate weather data at no cost. This data can support decisions such as inventory planning, logistics, and demand forecasting in regions where weather impacts operations, helping reduce costs and improve efficiency.

## Data Transformations Applied
- Column formatting:

Converted time to date format.

Ensured numeric columns (temperature_2m_max, temperature_2m_min, precipitation_sum) are floats.

Added a city column.

- Feature engineering:

Added day_of_week and month columns from time.

Calculated temperature_range (max - min).

Categorized rainfall into No rain, Light, Moderate, Heavy.

Calculated a 7-day rolling average of max temperature (temp7d_avg).

Missing values handling:

Filled numeric missing values with 0.

Filled categorical/missing string values with "unknown".

