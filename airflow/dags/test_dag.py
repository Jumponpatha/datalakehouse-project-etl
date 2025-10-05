import datetime
from airflow.sdk import dag
from airflow.providers.standard.operators.empty import EmptyOperator

# JOB: TEST_RUN_DAG_000
# DESCRIPTION: This DAG is for testing a simple run with an EmptyOperator

@dag(start_date=datetime.datetime(2021, 1, 1), schedule=None)

def test_generate_dag_000():
    # Define a single task that does nothing
    EmptyOperator(task_id="running_dag_task")

# Instantiate the DAG
test_generate_dag_000()