import datetime
from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator

# JOB: TEST_ETL_PYTHON_DAG_001
# DESCRIPTION: A simple test ETL DAG with Extract → Transform → Load with Python Operator.

@dag(
    dag_id="test_etl_python_dag_001",
    start_date=datetime.datetime(2025, 10, 5),
    schedule=None,
    catchup=False,
    tags=["test", "etl"],
    description="A simple test ETL DAG with Extract → Transform → Load"
)
def test_etl_dag():

    # Extract data
    @task(task_id="extract_data")
    def extract():
        print("Extracting data...")
        return {"data": [1, 2, 3]}

    # Transform data
    @task(task_id="transform_data")
    def transform(extracted_data):
        print(f"Transforming data: {extracted_data}")
        transformed = [x * 10 for x in extracted_data["data"]]
        return {"transformed_data": transformed}

    # Load data
    @task(task_id="load_data")
    def load(transformed_data):
        print(f"Loading data: {transformed_data}")

    # DAG Flow
    extracted = extract()
    transformed = transform(extracted)
    load_task = load(transformed)

    extracted >> transformed >> load_task

# Instantiate DAG
etl_dag = test_etl_dag()
