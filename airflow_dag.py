
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
import pandas as pd
from config import config
import psycopg2

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    def extract():
        fp = '//home//vishnud6//vgsales_project//cleaned_vgsales.csv'
    def transform():
        df = pd.read_csv('//home//vishnud6//vgsales_project//cleaned_vgsales.csv')
        
    def load():
   
        # Your loading code here
        """Connect to PostgreSQL database using psycopg2"""
        conn = None
        cur = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()

            data_to_load = pd.read_csv('//home//vishnud6//vgsales_project//cleaned_vgsales.csv')

            # Create the table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS vgsales_data (
                    "RANK" INTEGER,
                    "NAME" TEXT,
                    "PLATFORM" TEXT,
                    "YEAR" INTEGER,
                    "GENRE" TEXT,
                    "PUBLISHER" TEXT,
                    "NORTH AMERICA SALES" DOUBLE PRECISION,
                    "EUROPE SALES" DOUBLE PRECISION,
                    "JAPAN SALES" DOUBLE PRECISION,
                    "OTHER SALES" DOUBLE PRECISION,
                    "GLOBAL SALES" DOUBLE PRECISION
                )
            """)
            conn.commit()

            # Insert data into the table row by row
            for index, row in data_to_load.iterrows():
                rank = row.get('RANK', None)
                name = row.get('NAME', None)
                platform = row.get('PLATFORM', None)
                year = row.get('YEAR', None)
                genre = row.get('GENRE', None)
                publisher = row.get('PUBLISHER', None)
                na_sales = row.get('NORTH AMERICA SALES', None)
                eu_sales = row.get('EUROPE SALES', None)
                jp_sales = row.get('JAPAN SALES', None)
                other_sales = row.get('OTHER SALES', None)
                global_sales = row.get('GLOBAL SALES', None)
                
                if (rank is not None and name is not None and platform is not None and year is not None
                    and genre is not None and publisher is not None and na_sales is not None
                    and eu_sales is not None and jp_sales is not None and other_sales is not None
                    and global_sales is not None):
                    
                    cur.execute("""
                        INSERT INTO vgsales_data (
                            "RANK", "NAME", "PLATFORM", "YEAR", "GENRE", "PUBLISHER",
                            "NORTH AMERICA SALES", "EUROPE SALES", "JAPAN SALES",
                            "OTHER SALES", "GLOBAL SALES"
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (rank, name, platform, year, genre, publisher, na_sales,
                        eu_sales, jp_sales, other_sales, global_sales))
                    
                    print(f"Inserted row {index + 1} into the vgsales_data table")

            conn.commit()

        except psycopg2.Error as e:
            print("Error:", e)
            conn.rollback()  # Rollback changes if an error occurs

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
                print("Database connection closed")

    transform_task = PythonOperator(task_id='transform', python_callable=transform)
    load_task = PythonOperator(task_id='load', python_callable=load)

    extract_task >> transform_task >> load_task
