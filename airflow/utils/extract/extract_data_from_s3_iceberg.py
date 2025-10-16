import pyspark
import pandas as pd
import logging
from airflow.sdk import Variable
from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType, FloatType
from pyspark.sql import DataFrame as SparkDataFrame

# AWS MinIO - Credential
aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")

# Spark Session / Connection
def spark_conn():
    try:
        spark = (
            SparkSession.builder.appName("Load_data_into_S3")
            # .master('spark://c33c0029f634:7077')
            .config("spark.jars.packages",
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                    "software.amazon.awssdk:bundle:2.24.8,"
                    "software.amazon.awssdk:url-connection-client:2.24.8,"
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.375")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lakehouse_prod_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse_prod_catalog.type", "nessie")
            .config("spark.sql.catalog.lakehouse_prod_catalog.uri", "http://nessie:19120/api/v2")
            .config("spark.sql.catalog.lakehouse_prod_catalog.warehouse", "s3a://warehouse/")
            .config("spark.sql.catalog.lakehouse_prod_catalog.ref", "main")
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .getOrCreate()
        )

        # spark = (
        #             SparkSession.builder.appName("Load_data_into_S3")
        #             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        #             .config("spark.sql.catalog.lakehouse_prod_catalog", "org.apache.iceberg.spark.SparkCatalog")
        #             .config("spark.sql.catalog.lakehouse_prod_catalog.type", "nessie")
        #             .config("spark.sql.catalog.lakehouse_prod_catalog.uri", "http://nessie:19120/api/v2")
        #             .config("spark.sql.catalog.lakehouse_prod_catalog.ref", "main")
        #             .config("spark.sql.catalog.lakehouse_prod_catalog.warehouse", "s3a://warehouse/")
        #             .config("spark.sql.catalog.lakehouse_prod_catalog.s3.endpoint", "http://minio:9000")
        #             .config("spark.sql.catalog.lakehouse_prod_catalog.s3.path-style-access", "true")
        #             .config("spark.sql.catalog.lakehouse_prod_catalog.s3.access-key-id", aws_access_key_id)
        #             .config("spark.sql.catalog.lakehouse_prod_catalog.s3.secret-access-key", aws_secret_access_key)
        #             .config("spark.hadoop.fs.s3a.path.style.access", "true")
        #             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        #             .getOrCreate()
        #         )

        logging.info(f"SparkSession created successfully: {spark.version}")
        return spark
    except KeyError as e:
        logging.error("Cannot connect to Spark session: ", e)
        raise

# Load raw HRIS data with Spark
def load_data_into_s3_minio(data, preview_rows, mode, catalog, schema, table_name):
    """
    Load data into S3 MinIO via Spark + Iceberg.
    """

    # Spark Session
    spark = spark_conn()

    # Check DataFrame type to SparkDataFrame
    if isinstance(data, SparkDataFrame):
        logging.info(f"A table is SparkDataFrame.")
        df_spark = data
    elif isinstance(data, pd.DataFrame):
        logging.info(f"A table is Pandas. The process will transform into SparkDF")
        df_spark = spark.createDataFrame(data)
    else:
        logging.info("This is neither a Spark nor a Pandas DataFrame / No have Data")

    # Preview the data before load (Spark DataFrame)
    logging.info("Data schema:")
    df_spark.printSchema()
    logging.info(f"Preview first {preview_rows} rows:")
    df_spark.show(preview_rows, truncate=False)

    # Create Catalog and Database if not exists
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{schema};")
    logging.info(f"Namespace '{catalog}.{schema}' is READY.")
    # spark.sql(f"SHOW NAMESPACES IN {schema}").show()

    try:
        df_spark.write.mode(mode).format("parquet").saveAsTable(f"{catalog}.{schema}.{table_name}")
        logging.error(f"Successfully load data into {catalog}.{schema}.{table_name}")
    except Exception as e:
        logging.error(f"Failed to load data into {catalog}.{schema}.{table_name}: {e}")
        raise

    # Stop the Spark Session
    spark.stop()
    logging.info("SparkSession stopped.")