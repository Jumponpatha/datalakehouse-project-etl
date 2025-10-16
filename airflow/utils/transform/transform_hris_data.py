import pyspark
import pandas as pd
import logging
from airflow.sdk import Variable
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
import pyspark.sql.functions as F

# AWS MinIO - Credential
aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")

# Spark Session / Connection
def spark_conn():
    try:
        spark = (
            SparkSession.builder.appName("Transform_data_s3")
            # .master('spark://c33c0029f634:7077')
            .config("spark.jars.packages",
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                    "software.amazon.awssdk:bundle:2.24.8,"
                    "software.amazon.awssdk:url-connection-client:2.24.8,"
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.375")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lakehouse_prod_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse_prod_catalog.type", "nessie")  # <--- use nessie, not rest
            .config("spark.sql.catalog.lakehouse_prod_catalog.uri", "http://nessie:19120/api/v1")  # <--- correct URL
            .config("spark.sql.catalog.lakehouse_prod_catalog.warehouse", "s3a://warehouse/")
            .config("spark.sql.catalog.lakehouse_prod_catalog.ref", "main")  # <--- default Nessie branch
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")   # MinIO endpoint
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)            # MinIO access key
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)            # MinIO secret key
            .config("spark.hadoop.fs.s3a.path.style.access", "true")           # important for MinIO
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .getOrCreate()
        )
        logging.info(f"SparkSession created successfully: {spark.version}")
        return spark
    except KeyError as e:
        logging.error("Cannot connect to Spark session: ", e)
        raise

# Transform HRIS data with Spark
def transfrom_hris_data(data):

    # Spark Session
    spark = spark_conn()

    # Check DataFrame type to SparkDataFrame
    if isinstance(data, SparkDataFrame):
        logging.info(f"A table is SparkDataFrame.")
        df_spark = data
    elif isinstance(data, pd.DataFrame):
        print(f"A table is Pandas. The process will transform into SparkDF")
        df_spark = spark.createDataFrame(data)
    else:
        print("This is neither a Spark nor a Pandas DataFrame / No have Data")

    # Count duplicates
    dup_count_df = df_spark.groupBy(df_spark.columns).count() \
        .withColumn("dup_count", F.col("count") - 1) \
        .filter(F.col("dup_count") > 0) \
        .agg(F.sum("dup_count").alias("duplicate_count"))
    duplicate_count = dup_count_df.collect()[0]["duplicate_count"] or 0
    print(f"Number of duplicate rows in {df_spark}: {duplicate_count}")

    # Drop Duplicated
    df_drop_dup = df_spark.dropDuplicates()

    # Drop NULL in 'Ingested_Time' column
    cleaned_df_spark = df_drop_dup.dropna(subset=["Ingested_Time"])

    # Stop Spark Session
    spark.stop()
    logging.info("SparkSession stopped.")

    return cleaned_df_spark