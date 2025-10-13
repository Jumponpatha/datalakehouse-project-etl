import boto3
import pandas as pd
from datetime import datetime
import pytz
import logging
from io import StringIO
from airflow.sdk import Variable


# The standard IANA timezone identifier for Thailand
th_tz = pytz.timezone('Asia/Bangkok')

# Format the datetime object, including the timezone name (%Z)
# Note: pytz timezones often return the full identifier (e.g., 'Asia/Bangkok') for %Z,
# or a standard abbreviation like 'ICT' depending on the Python version/system configuration.

# Get the current time in the specified timezone
start_etl_time = datetime.now(th_tz).strftime("%Y-%m-%d %H:%M:%S")

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
    start_extract_data = datetime.now(th_tz).strftime("%Y-%m-%d %H:%M:%S")

    # S3 Connection / Key
    s3 = s3_client()
    key = f"{folder}/{filename}"

    logging.info(f"Start Extract time: {start_extract_data}")
    logging.info(f"Extract csv data from MinIO: s3a://{bucket}/{key}")

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(content))
        df['Ingested_Time'] = start_extract_data
        print(f'Print the Sample data:')
        print(df.head(5))

        end_extract_data = datetime.now(th_tz).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"Successfully extract {filename} with {len(df)} rows.")
        logging.info(f"Finish Extract time: {end_extract_data}")
        return df

    except s3.exceptions.NoSuchKey:
        end_extract_data = datetime.now(th_tz).strftime("%Y-%m-%d %H:%M:%S")
        logging.error(f"File not found: s3a://{bucket}/{key}")
        logging.info(f"Stop Extract time: {end_extract_data}")
        raise FileNotFoundError(f"The file '{key}' does not exist in bucket '{bucket}'")

    except Exception as e:
        end_extract_data = datetime.now(th_tz).strftime("%Y-%m-%d %H:%M:%S")
        logging.error(f"Error reading from MinIO: {e}")
        logging.info(f"Stop Extract time: {end_extract_data}")
        raise