from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

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
    dag_id="COVID-19_ETL",
    default_args=default_args,
    description="this first dag",
    schedule_interval="@daily",
    start_date=datetime.today()
) as dag:
    @task
    def extract():
        link = 'https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-daily-data.csv'

        file_name = 'WHO-COVID-19-global-daily-data.csv'
        file_path = os.path.join(file_name)

        response = requests.get(link)

        if response.status_code == 200:
            
            # Save the content to the specified file
            with open(file_path, 'wb') as file:
                file.write(response.content)
                logging.info(f'CSV downloaded to {file_path}')
            return file_path
        
        else:
            raise Exception(f"Failed to download CSV: {response.status_code}")


    @task
    def transform(file_path='WHO-COVID-19-global-daily-data.csv'):
        
        try:
            df = pd.read_csv(file_path)
        except FileNotFoundError:
            logging.error(f"File not found: {file_path}")
            raise
       
        df["Date_reported"] = pd.to_datetime(df.Date_reported)

        # fill missing value in country code
        # nambia country code is NA
        df["Country_code"] = df["Country_code"].fillna('NA')

        # fill the missing value where cummulative counterpart = 0
        df.loc[(df['New_cases'].isna()) & (df['Cumulative_cases'].eq(0)), 'New_cases'] = 0
        df.loc[(df['New_deaths'].isna()) & (df['Cumulative_deaths'].eq(0)), 'New_deaths'] = 0

        def fill_new_col(df, new_col, cumulative_col):
            # Create a copy of the dataframe to avoid modifying the original
            result_df = df.copy()
            
            # Initialize the new column if it doesn't exist
            if new_col not in result_df.columns:
                result_df[new_col] = None
            
            # Process each country separately
            for country in result_df['Country'].unique():
                # Create a mask for the current country
                mask = result_df['Country'] == country
                
                # Get the country-specific data
                country_data = result_df.loc[mask]
                
                # Calculate differences and fill values
                result_df.loc[mask, new_col] = country_data[cumulative_col].diff()
                
                # Fill first value (which will be NaN after diff()) with the cumulative value
                first_idx = country_data.index[0]
                result_df.loc[first_idx, new_col] = result_df.loc[first_idx, cumulative_col]
            
            return result_df


        df_final = fill_new_col(df, new_col='New_cases', cumulative_col='Cumulative_cases')
        df_final = fill_new_col(df_final, new_col='New_deaths', cumulative_col='Cumulative_deaths')

        df_final['year'] = df_final['Date_reported'].dt.year
        df_final['month'] = df_final['Date_reported'].dt.month
        df_final['day'] = df_final['Date_reported'].dt.day

        return df_final


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
                
                # date dimension
                cursor.execute("""
                    Create table if not exists Date_dim (
                            "DateID" Serial primary key,
                            "Date_reported" DATE,
                            "year" SMALLINT,
                            "month" SMALLINT,
                            "day" SMALLINT
                            );
                """)
                
                cursor.execute("""
                    INSERT INTO Date_dim ("Date_reported", "year", "month", "day")
                    SELECT "Date_reported", "year", "month", "day" FROM main;
                """)
                
                # location dimension
                cursor.execute("""
                    Create table if not exists Location_dim (
                            "LocationID" Serial primary key,
                            "Country" TEXT, 
                            "Country_code" TEXT, 
                            "WHO_region" TEXT
                            );
                            """)
                
                cursor.execute("""
                    INSERT INTO Location_dim ("Country", "Country_code", "WHO_region")
                    SELECT "Country", "Country_code", "WHO_region" FROM main;
                """)

                # fact table
                cursor.execute("""
                    Create table if not exists COVID_fact (
                            "FactID" Serial primary key,
                            "DateID" INT,
                            FOREIGN KEY ("DateID") REFERENCES Date_dim("DateID"),
                            "LocationID" INT,
                            FOREIGN KEY ("LocationID") REFERENCES Location_dim("LocationID"),
                            "New_cases" INT, 
                            "New_deaths" INT, 
                            "Cumulative_cases" INT, 
                            "Cumulative_deaths" INT
                            );
                """)

                cursor.execute("""
                    INSERT INTO COVID_fact ("New_cases", "New_deaths", "Cumulative_cases", "Cumulative_deaths")
                    SELECT "New_cases", "New_deaths", "Cumulative_cases", "Cumulative_deaths" FROM main;
                """)

                # drop main table
                cursor.execute("""
                    DROP TABLE main;
                """)

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


    task1 = extract()
    task2 = transform(task1)
    load(task2)
