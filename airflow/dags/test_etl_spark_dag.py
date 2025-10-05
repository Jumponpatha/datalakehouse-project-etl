import datetime
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

@dag(
    dag_id="test_spark_airflow_dag_002",
    start_date=datetime.datetime(2025, 10, 5),
    schedule="@daily",
    catchup=False,
    tags=["test", "spark"]
)
def test_spark_dag():
    spark_task = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/airflow/spark_jobs/spark_test.py",
        name="test_spark_airflow",
        conn_id="spark_session",  # default Spark connection in Airflow
        verbose=True
    )

    spark_task

# Instantiate DAG
etl_spark_dag = test_spark_dag()
