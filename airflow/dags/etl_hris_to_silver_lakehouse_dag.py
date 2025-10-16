import datetime
from utils.extract.extract_raw_from_s3_lake import extract_csv_from_lake_s3
from utils.load.load_data_to_s3 import load_data_into_s3_minio
from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator

@dag(
    dag_id="etl_hris_to_silver_lakehouse_dag",
    start_date=datetime.datetime(2025, 10, 5),
    schedule='* 30 * * *',
    catchup=False,
    tags=["HRIS","lakehouse", "etl", "transformed", "silver"],
    description="ETL process to ingest core Human Resources Management System (HRMS) tables \
    into the raw and standardized layers of the Data Lakehouse",
    default_args={"retries": 2},
)

def etl_hris_to_silver_lakehouse_dag():

    # Extract data
    @task(task_id="extract_hris_data")
    def extract():
        bucket = 'landing-data'
        folder = 'HR-LAKE-DATASET'
        filename = 'HRIS_EMPLOYEE_20250810.csv'

        df = extract_csv_from_lake_s3(bucket=bucket, folder=folder, filename=filename)
        return df

    # Load data
    @task(task_id="load_data_into_s3")
    def load(transformed_data):
        print(f"Loading data: {transformed_data}")
        load_data_into_s3_minio(
            data=transformed_data,
            preview_rows=5,
            mode='overwrite',
            catalog='lakehouse_prod_catalog',
            schema='raw_bronze_zone',
            table_name='raw_bronze_hris_employee'
        )

    # DAG Flow
    extract_df = extract()
    load(extract_df)

    # extract >> load

# Instantiate DAG
etl_dag = etl_hris_to_silver_lakehouse_dag()