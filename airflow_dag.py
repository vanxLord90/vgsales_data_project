from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from config import config

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract(ti):
    # Extract data from source (e.g., CSV file)
    fp = '/home/vishnud6/vgsales_project/vgsales.csv'
    ti.xcom_push(key='file_path', value=fp)

def transform(ti):
    # Transform data (e.g., clean up, preprocess)
    fp = ti.xcom_pull(key='file_path', task_ids='extract')
    vgsales = pd.read_csv(fp)
    initial_shape = vgsales.shape
    print(initial_shape)
    vgsales_clean = vgsales.drop_duplicates().dropna()
    new_shape = vgsales_clean.shape
    print(new_shape)
    cleaned_fp = '/home/vishnud6/vgsales_project/vgsales_clean.csv'
    vgsales_clean["Year"]= vgsales_clean["Year"].astype(int)
    vgsales_clean['Name'] = vgsales_clean['Name'].str.replace(',', ' ')
    vgsales_clean['Publisher'] = vgsales_clean['Publisher'].str.replace('["|,]', '', regex=True)

    vgsales_clean.to_csv(cleaned_fp, index=False)
    ti.xcom_push(key='cleaned_file_path', value=cleaned_fp)

def load(ti):
    # Load transformed data into PostgreSQL database
    fp = ti.xcom_pull(key='cleaned_file_path', task_ids='transform')
    params = config()
    conn = psycopg2.connect(**params)
    cur = conn.cursor()

    # Load data from CSV into the table
    with open(fp, 'r') as f:
        next(f)  # Skip header
        cur.copy_from(f, 'vgsales_data', sep=',')
    conn.commit()

    cur.close()
    conn.close()
    print("Data loaded into PostgreSQL")

with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load
    )

    extract_task >> transform_task >> load_task
