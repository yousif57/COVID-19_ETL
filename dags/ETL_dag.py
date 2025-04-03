from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from transform import data_transform

import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta


default_args = {
    'owner': 'jou',
    'retries':5,
    "retry_delay":timedelta(seconds=30)
}

POSTGRES_CONN_ID='covid_conn'


with DAG(
    dag_id="ETL_dag",
    default_args=default_args,
    schedule_interval="@weekly",
    start_date=datetime.today()
) as dag:
    @task
    def extract_transform():
        link = 'https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-daily-data.csv'

        current_date = datetime.now().strftime("%Y-%m-%d")  # Format: YYYY-MM-DD
        
        file_name = f'WHO-COVID-19-global-daily-{current_date}.csv'
        path = '/opt/airflow/data'

        file_path = os.path.join(path, file_name)


        if file_name in os.listdir(path):
            logging.info("data is up to date")
            return data_transform(file_path)
    
        else:
            logging.info("remove old data")
            for file in os.listdir(path):
                os.remove(os.path.join(path, file))


            logging.info("downloading latest data")
            response = requests.get(link)
            if response.status_code == 200:
                # Save the content to the specified file
                with open(file_path, 'wb') as file:
                    file.write(response.content)
                    logging.info(f'CSV downloaded to {file_path}')

                return data_transform(file_path)
            
            else:
                raise Exception(f"Failed to download CSV: {response.status_code}")

        
    @task
    def load(df):

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        logger = logging.getLogger(__name__)

        try:
            engine = pg_hook.get_sqlalchemy_engine()

            df.to_sql(
            name='main',
            con=engine,
            if_exists='replace',  # Options: 'fail', 'replace', 'append'
            index=False,
            schema='public'
            )

            conn = pg_hook.get_conn()
            cursor = conn.cursor()

            try:
                conn.autocommit = False
               

# --------------------------------------------------------------
#                      date dimension
# --------------------------------------------------------------

                # date dimension
                logging.info("create dim_date table")

                cursor.execute("DROP TABLE IF EXISTS dim_date;")
                cursor.execute("""
                CREATE TABLE dim_date (
                    date_key INT PRIMARY KEY, -- Primary key column
                    date DATE,
                    year INT,
                    month INT,
                    month_name TEXT,
                    day INT,  
                    day_name TEXT,
                    quarter INT,

                    day_week_number INT,
                    week_number INT,

                    weekend BOOLEAN,
                    weekday BOOLEAN,
                    
                    year_month TEXT,
                    year_quarter TEXT
                )
                """)

                logging.info("populate dim_date table")
                cursor.execute("""
                INSERT INTO dim_date
                SELECT
                    TO_CHAR(date_sequence, 'YYYYMMDD')::INT as date_key,
                    date_sequence::date AS date,
                    EXTRACT(YEAR FROM date_sequence) AS year,
                    EXTRACT(MONTH FROM date_sequence) AS month,
                    TO_CHAR(date_sequence, 'Month') AS month_name, -- Full month name
                    EXTRACT(DAY FROM date_sequence) AS day,
                    TO_CHAR(date_sequence, 'Day') AS day_name,     -- Full day name
                    
                    EXTRACT(QUARTER FROM date_sequence) AS quarter, -- Quarter (1-4)

                    EXTRACT(DOW FROM date_sequence) AS day_week_number,
                    EXTRACT(WEEK FROM date_sequence) AS week_number, -- ISO week number
                    
                    EXTRACT(DOW FROM date_sequence) IN (0, 6) AS weekend, -- 0 = Sunday, 6 = Saturday
                    EXTRACT(DOW FROM date_sequence) IN (1,2,3,4,5) AS weekday,

                    
                    TO_CHAR(date_sequence, 'YYYY-MM') AS year_month, -- Year-Month format
                    CONCAT(EXTRACT(YEAR FROM date_sequence), ' Q', EXTRACT(QUARTER FROM date_sequence)) AS year_quarter -- Year-Quarter format
                
                FROM (
                    SELECT generate_series(
                            min("Date_reported")::date,
                            max("Date_reported")::date,
                            '1 day'::interval
                        ) AS date_sequence from main
                );
                """)

# --------------------------------------------------------------
#                      location dimension
# --------------------------------------------------------------

                # location dimension
                logging.info("create location_dim table")

                cursor.execute("DROP TABLE IF EXISTS dim_location;")              
                cursor.execute("""
                Create table dim_location (
                        country TEXT, 
                        country_code TEXT, 
                        WHO_region TEXT
                        );
                        """)

                logging.info("populate location_dim table")
                cursor.execute("""
                    INSERT INTO dim_location (country, country_code, WHO_region)
                    SELECT DISTINCT "Country", "Country_code", "WHO_region" FROM main;
                """)

                logging.info("add primary of location_dim table")
                cursor.execute("""
                    ALTER TABLE dim_location 
                    ADD COLUMN location_key SERIAL PRIMARY KEY;
                """)

# --------------------------------------------------------------
#                      fact table
# --------------------------------------------------------------

                logging.info("create fact table")

                cursor.execute("DROP TABLE IF EXISTS fact_covid;")        
                cursor.execute("""
                    CREATE Table fact_covid(
                    id SERIAL PRIMARY KEY,
                    date_key INT,
                    location_key INT,
                    New_cases INT,
                    Cumulative_cases INT,
                    New_deaths INT,
                    Cumulative_deaths INT,
                    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
                    FOREIGN KEY (location_key) REFERENCES dim_location(location_key)
                    );
                    """)

                logging.info("populate fact table")
                cursor.execute("""                
                        INSERT INTO fact_covid(date_key, 
                        location_key, New_cases, Cumulative_cases, 
                        New_deaths, Cumulative_deaths)
                        SELECT date_key, location_key, "New_cases", 
                        "Cumulative_cases", "New_deaths", "Cumulative_deaths" 
                        FROM main;
                """)

                logging.info("drop main table")
                cursor.execute("DROP TABLE main;")        


                conn.commit()

            except Exception as e:
                conn.rollback()
                logger.error(f"Error during database operations: {str(e)}")
                raise
            
            finally:
                cursor.close()
                conn.close()
        
        except Exception as e:
            logger.error(f"Error during initial data load: {str(e)}")
            raise

        finally:
            logging.info("LOADING data completed")


    task1 = extract_transform()
    load(task1)
