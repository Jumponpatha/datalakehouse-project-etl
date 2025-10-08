import datetime
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

@dag(
    dag_id="test_etl_spark_airflow_dag_003",
    start_date=datetime.datetime(2025, 10, 5),
    schedule=None,
    catchup=False,
    tags=["test", "spark"]
)
def test_etl_spark_dag():
    spark_task = SparkSubmitOperator(
        task_id="run_etl_spark_job",
        application="/opt/airflow/utils/test/test_etl_spark.py",
        name="test_etl_spark_airflow",
        conn_id="spark_session",  # default Spark connection in Airflow
        verbose=True
    )

    spark_task

# Instantiate DAG
etl_spark_test_dag = test_etl_spark_dag()
