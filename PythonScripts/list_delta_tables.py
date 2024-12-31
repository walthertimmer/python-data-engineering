import os
import logging
from typing import Optional
import boto3
from dotenv import load_dotenv
from delta.tables import DeltaTable

try:
    load_dotenv()
except Exception as e:
    print(f"Failed to load .env file: {str(e)}")

def get_env_var(var_name: str, default: Optional[str] = None) -> str:
    """Get environment variable with optional default value"""
    value = os.environ.get(var_name, default)
    if value is None:
        raise ValueError(f"Environment variable {var_name} not set")
    return value

def get_s3_client():
    """Create S3 client with custom endpoint"""
    s3_access_key = get_env_var("S3_ACCESS_KEY_ID")
    s3_secret_key = get_env_var("S3_SECRET_ACCESS_KEY")
    s3_endpoint_url = get_env_var("S3_ENDPOINT_URL")
    
    return boto3.client(
        's3',
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        endpoint_url=s3_endpoint_url,
        verify=True  # SSL verification
    )

def list_delta_tables(bucket_name, prefix=''):
    """List all Delta tables in the specified S3 bucket and prefix"""
    s3_client = get_s3_client()
    tables = set()
    
    logging.info(f"Searching for Delta tables in bucket: {bucket_name} with prefix: {prefix}")
    
    # List objects with the given prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            logging.info(f"Processing page: {page.get('KeyCount', 0)} objects")
            
            if 'Contents' not in page:
                logging.warning(f"No contents found in page")
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                logging.debug(f"Checking key: {key}")
                # Look for _delta_log directories
                if '_delta_log' in key:
                    # Get the parent directory of _delta_log
                    table_path = key.split('/_delta_log')[0]
                    logging.debug(f"Found Delta table: {table_path}")
                    tables.add(f"s3a://{bucket_name}/{table_path}")
    except Exception as e:
        logging.error(f"Error listing objects: {str(e)}")
        raise
    return sorted(list(tables))

def main() -> None:
    logging.basicConfig(level=logging.INFO)
    
    bucket_name = get_env_var("S3_BUCKET", "datahub") # S3 bucket name
    prefix = 'warehouse/'
    tables = list_delta_tables(bucket_name,prefix=prefix)
    
    logging.info(f"Found {len(tables)} Delta tables")
    for table in tables:
        logging.info(f"Found Delta table: {table}")

if __name__ == "__main__":
    main()
