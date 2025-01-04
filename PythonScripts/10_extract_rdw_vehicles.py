"""extract RDW car data
https://opendata.rdw.nl/Voertuigen/Open-Data-RDW-Gekentekende_voertuigen/m9d7-ebf2/about_data
https://dev.socrata.com/foundry/opendata.rdw.nl/m9d7-ebf2
api endpoint: 
- https://opendata.rdw.nl/resource/m9d7-ebf2.json
CSV endpoint: 
- https://opendata.rdw.nl/api/views/m9d7-ebf2/rows.csv?accessType=DOWNLOAD&api_foundry=true
±16.5M rows
https://opendata.rdw.nl/profile/edit/developer_settings

Get registered vehicle data from RDW (Dutch Vehicle Authority).
Uses pagination to handle the large dataset efficiently.
"""

import logging
import os
import sys
from datetime import datetime
from typing import Optional, List, Dict
import json
import requests
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

try:
    load_dotenv()
except Exception as e:
    print(f"Failed to load .env file: {str(e)}")

def setup_logging(log_level: str = "INFO") -> None:
    """Configure logging with timestamp, level and message"""
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

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
        verify=True
    )

def get_last_run_timestamp(bucket_name, prefix):
    """Get timestamp of last successful run from S3"""
    try:
        timestamp_file = f"{prefix}last_run_timestamp.txt"
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=bucket_name, Key=timestamp_file)
        return response['Body'].read().decode('utf-8')
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            # File doesn't exist yet - this is likely the first run
            logging.info("No previous timestamp found - this appears to be the first run")
            return None
        else:
            # Some other error occurred
            logging.error("Error reading timestamp file: %s", str(e))
            raise

def save_last_run_timestamp(bucket_name, prefix):
    """Save current timestamp to S3"""
    timestamp = datetime.now().strftime("%Y-%m-%d")
    timestamp_file = f"{prefix}last_run_timestamp.txt"

    s3_client = get_s3_client()
    s3_client.put_object(
        Bucket=bucket_name,
        Key=timestamp_file,
        Body=timestamp.encode('utf-8')
    )

def fetch_rdw_data(base_url: str, offset: int, limit: int = 1000) -> List[Dict]:
    """Fetch data from RDW API with pagination"""
    # base_url = "https://opendata.rdw.nl/resource/m9d7-ebf2.json"

    app_token = get_env_var("RDW_APP_TOKEN")

    headers = {
        'X-App-Token': app_token
    }

    params = {
        "$offset": offset,
        "$limit": limit
    }

    response = requests.get(
        base_url,
        params=params,
        headers=headers,
        timeout=60)
    response.raise_for_status()

    return response.json()

def save_to_s3(data: str, bucket_name: str, object_name: str) -> None:
    """Save data to S3 bucket"""
    try:
        logger = logging.getLogger(__name__)
        s3_client = get_s3_client()

        logger.info("Uploading to s3://%s/%s", bucket_name, object_name)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=data
        )
        logger.info("Successfully uploaded to s3://%s/%s", bucket_name, object_name)

    except Exception as e:
        logger.error(f"Failed to upload to S3: {str(e)}")
        raise

def main() -> None:
    """Main function to extract RDW data and save to S3"""
    try:
        # Setup logging
        setup_logging(log_level="INFO")
        logger = logging.getLogger(__name__)

        # Log script start
        logger.info("Starting RDW data extraction")
        start_time = datetime.now()

        # Get configuration
        bucket_name = get_env_var("S3_BUCKET", "datahub")
        target_prefix = get_env_var("TARGET_LOCATION", "raw/rdw-vehicles/")
        batch_size = 1000

        # Get last run timestamp
        last_run = get_last_run_timestamp(bucket_name, target_prefix)

        # Initialize variables
        offset = 0
        all_data = []

        # Modify your API call to include date filter
        base_url = "https://opendata.rdw.nl/resource/m9d7-ebf2.json"
        if last_run:
            # Add filter for datum_tenaamstelling greater than last run
            query = f"?$where=datum_tenaamstelling > '{last_run}'"
            url = base_url + query
        else:
            url = base_url

        while True:
            try:
                # Fetch batch of data
                logger.info("Fetching records %d to %d", offset, offset + batch_size)
                batch = fetch_rdw_data(url, offset, batch_size)

                if not batch:  # No more data
                    break

                all_data.extend(batch)
                offset += batch_size

                # Save intermediate results every 100K records
                if len(all_data) % 100000 == 0:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    intermediate_file = f"{target_prefix}rdw_vehicles_batch_{offset}_{timestamp}.json"
                    save_to_s3(
                        json.dumps(all_data),
                        bucket_name,
                        intermediate_file
                    )
                    all_data = []  # Clear memory

            except requests.exceptions.RequestException as e:
                logger.error("API request failed at offset %d: %s", offset, str(e))
                raise

        # Save any remaining data
        if all_data:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            final_file = f"{target_prefix}rdw_vehicles_final_{timestamp}.json"
            save_to_s3(
                json.dumps(all_data),
                bucket_name,
                final_file
            )
            save_last_run_timestamp(bucket_name, target_prefix)

        # Log completion
        duration = datetime.now() - start_time
        logger.info("Process completed successfully in %s", duration)
        sys.exit(0)

    except Exception as e:
        logger.error("Process failed: %s", str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
