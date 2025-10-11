import datetime
from utils.extract.extract_raw_hris import extract_csv_from_lake_s3
from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
bucket = "landing-data"
folder = "HR-LAKE-DATASET"
file = "HRIS_EMPLOYEE_20250810.csv"
@dag(
    dag_id="etl_hris_to_lakehouse",
    start_date=datetime.datetime(2025, 10, 5),
    schedule='@weekly',
    catchup=False,
    tags=["HRIS","lakehouse", "etl", "raw", "silver", "gold"],
    description="ETL process to ingest core Human Resources Management System (HRMS) tables \
    into the raw and standardized layers of the data Lakehouse"
)

def test_etl_dag():

    # Extract data
    @task(task_id="extract_hris_data")
    def extract():
        bucket= 'landing-data'
        folder= 'HR-LAKE-DATASET'
        filename= 'HRIS_EMPLOYEE_20250810.csv'
        df = extract_csv_from_lake_s3(bucket, folder, filename)
        df['']
        df.head()
        return df

    # # Transform data
    # @task(task_id="transform_data")
    # def transform(extracted_data):
    #     print(f"Transforming data: {extracted_data}")
    #     transformed = [x * 10 for x in extracted_data["data"]]
    #     return {"transformed_data": transformed}

    # # Load data
    # @task(task_id="load_data")
    # def load(transformed_data):
    #     print(f"Loading data: {transformed_data}")

    # DAG Flow
    extract()
    # transformed = transform(extracted)
    # load_task = load(transformed)

    # extracted >> transformed >> load_task

# Instantiate DAG
etl_dag = test_etl_dag()