from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Iceberg-Nessie-Lakehouse-Test")
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
            "software.amazon.awssdk:bundle:2.24.8,"
            "software.amazon.awssdk:url-connection-client:2.24.8")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse_prod_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse_prod_catalog.type", "nessie")  # <--- use nessie, not rest
    .config("spark.sql.catalog.lakehouse_prod_catalog.uri", "http://nessie:19120/api/v2")  # <--- correct URL
    .config("spark.sql.catalog.lakehouse_prod_catalog.warehouse", "s3://warehouse/wh/")
    .config("spark.sql.catalog.lakehouse_prod_catalog.ref", "main")  # <--- default Nessie branch
    .getOrCreate()
)

df = spark.read.parquet("/home/iceberg/data/yellow_tripdata_2021-04.parquet")
df.show(10)


spark.stop()