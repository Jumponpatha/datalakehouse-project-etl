import datetime
from utils.extract.extract_raw_from_s3_lake import extract_csv_from_lake_s3
from utils.load.load_data_to_s3 import load_data_into_s3_minio
from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator

@dag(
    dag_id="etl_hris_to_lakehouse",
    start_date=datetime.datetime(2025, 10, 5),
    schedule='@weekly',
    catchup=False,
    tags=["HRIS","lakehouse", "etl", "raw", "bronze"],
    description="ETL process to ingest core Human Resources Management System (HRMS) tables \
    into the raw and standardized layers of the data Lakehouse"
)

def test_etl_dag():

    # Extract data
    @task(task_id="extract_hris_data")
    def extract():
        bucket = 'landing-data'
        folder = 'HR-LAKE-DATASET'
        filename = 'HRIS_EMPLOYEE_20250810.csv'

        df = extract_csv_from_lake_s3(bucket=bucket, folder=folder, filename=filename)
        return df
    # Load data
    @task(task_id="load_data")
    def load(transformed_data):
        print(f"Loading data: {transformed_data}")
        load_data_into_s3_minio

    # DAG Flow

    extract_df = extract()
    load(extract_df)

    extract >> load

# Instantiate DAG
etl_dag = test_etl_dag()