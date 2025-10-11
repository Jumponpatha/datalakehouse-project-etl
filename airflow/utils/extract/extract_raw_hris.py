import boto3
import pandas as pd
from io import StringIO
from airflow.sdk import Variable
import logging

def s3_client():
    """
        Get Credential for S3 Clint (MinIO)
    """
    try:
        aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
    except KeyError as e:
        logging.error("Cannot find Airflow Variable: %s", e)
        raise

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url="http://minio:9000",  # MinIO endpoint
        use_ssl=False
    )
    return s3

def extract_csv_from_lake_s3(bucket: str, folder:str, filename:str) -> pd.DataFrame:
    """
        Read a CSV file from MinIO (S3-compatible storage) into a Pandas DataFrame.
    """

    # S3 Connection / Key
    s3 = s3_client()
    key = f"{folder}/{filename}"

    logging.info(f"Reading file from MinIO: s3a://{bucket}/{key}")

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(content))
        logging.info(f"Successfully loaded {filename} with {len(df)} rows.")
        return df

    except s3.exceptions.NoSuchKey:
        logging.error(f"File not found: s3a://{bucket}/{key}")
        raise FileNotFoundError(f"The file '{key}' does not exist in bucket '{bucket}'")
    except Exception as e:
        logging.error(f"Error reading from MinIO: {e}")
        raise