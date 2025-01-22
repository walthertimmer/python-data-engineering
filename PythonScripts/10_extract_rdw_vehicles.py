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
If there was a successful run only <1 month data will be fetched.
"""

import logging
import os
import sys
import io
from datetime import datetime
from typing import Optional, List, Dict, Union
import time
import requests
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
from dateutil.relativedelta import relativedelta
import polars as pl

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
    logger = logging.getLogger(__name__)
    try:
        logger.info("Checking for last run timestamp")
        timestamp_file = f"{prefix}last_run_timestamp.txt"
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=bucket_name, Key=timestamp_file)
        return response['Body'].read().decode('utf-8')
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            # File doesn't exist yet - this is likely the first run
            logger.info("No previous timestamp found - this appears to be the first run")
            return None
        else:
            # Some other error occurred
            logger.error("Error reading timestamp file: %s", str(e))
            raise

def save_last_run_timestamp(bucket_name, prefix):
    """Save current timestamp to S3"""
    logger = logging.getLogger(__name__)
    timestamp = datetime.now().strftime("%Y-%m-%d")
    timestamp_file = f"{prefix}last_run_timestamp.txt"
    logger.info("Saving last run timestamp to S3: %s", timestamp)
    s3_client = get_s3_client()
    s3_client.put_object(
        Bucket=bucket_name,
        Key=timestamp_file,
        Body=timestamp.encode('utf-8')
    )
    logger.info("Timestamp saved successfully")

def get_date_filter(last_run: str) -> str:
    """Create date filter with 1 month overlap to catch delayed entries"""
    logger = logging.getLogger(__name__)
    logger.info("Checking for last run timestamp")
    if last_run:
        # Convert last_run to date and subtract 1 month for overlap
        last_run_date = datetime.strptime(last_run, "%Y-%m-%d")
        overlap_date = (last_run_date - relativedelta(months=1))
        # Format date as YYYYMMDD without dashes for RDW API
        formatted_date = overlap_date.strftime("%Y%m%d")
        logger.info("Fetching data since %s", formatted_date)
        return f"?$where=datum_eerste_toelating >= '{formatted_date}'"
    return ""

def fetch_rdw_data(
    base_url: str, 
    offset: int, 
    limit: int = 1000, 
    max_retries: int = 3) -> List[Dict]:
    """Fetch data from RDW API with pagination and small sleep
    
    Returns:
        Optional[List[Dict]]: List of records if successful, None if all retries failed
    """
    logger = logging.getLogger(__name__)
    app_token = get_env_var("RDW_APP_TOKEN")

    headers = {
        'X-App-Token': app_token
    }

    params = {
        "$offset": offset,
        "$limit": limit
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(
                base_url,
                params=params,
                headers=headers,
                timeout=60)
            response.raise_for_status()
            time.sleep(0.1)  # Add delay between requests to avoid overwhelming API
            return response.json()
        except (requests.Timeout, requests.RequestException) as e:
            wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
            if attempt < max_retries - 1:  # Don't log warning on last attempt
                logger.warning(
                    "Request failed (attempt %d of %d), retrying in %d seconds. "
                    "Error: %s",
                    attempt + 1,
                    max_retries,
                    wait_time,
                    str(e)
                )
                time.sleep(wait_time)
            else:
                logger.error(
                    "Request failed after %d attempts. Error: %s",
                    max_retries,
                    str(e)
                )
                raise
    return None  # Added explicit return None for when all retries fail

def save_to_s3(
    data: Union[str, bytes], 
    bucket_name: str, 
    object_name: str) -> None:
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

def save_checkpoint(bucket_name: str, prefix: str, offset: int) -> None:
    """Save current processing offset to S3"""
    logger = logging.getLogger(__name__)
    logger.info("Saving checkpoint to S3 at offset %d", offset)
    checkpoint_file_name = f"{prefix}checkpoint.txt"
    checkpoint_data = f"{offset}\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    s3_client = get_s3_client()
    s3_client.put_object(
        Bucket=bucket_name,
        Key=checkpoint_file_name,
        Body=checkpoint_data
    )
    logger.info("Saving checkpoint to S3 successful")

def get_checkpoint(bucket_name: str, prefix: str) -> int:
    """Get last saved offset from checkpoint"""
    logger = logging.getLogger(__name__)
    try:
        checkpoint_file = f"{prefix}checkpoint.txt"
        s3_client = get_s3_client()
        logger.info("Checking for checkpoint file at %s", checkpoint_file)
        response = s3_client.get_object(Bucket=bucket_name, Key=checkpoint_file)
        checkpoint_data = response['Body'].read().decode('utf-8')
        # Get first line containing offset
        offset = int(checkpoint_data.split('\n')[0])
        return offset
    except s3_client.exceptions.NoSuchKey:
        logger.info("No checkpoint found, starting from beginning")
        return 0
    except Exception as e:
        logger.error("Error reading checkpoint: %s", str(e))
        return 0
    
def cleanup_checkpoint(bucket_name: str, prefix: str) -> None:
    """Delete checkpoint file after successful run"""
    logger = logging.getLogger(__name__)
    checkpoint_file = f"{prefix}checkpoint.txt"
    try:
        logger.info("Cleaning up Checkpoint file at %s", checkpoint_file)
        s3_client = get_s3_client()
        s3_client.delete_object(Bucket=bucket_name, Key=checkpoint_file)
        logger.info("Checkpoint file cleaned up successfully")
    except Exception as e:
        logger.warning("Failed to cleanup checkpoint file: %s", str(e))

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
        batch_size = 10000

        # Get last run timestamp
        last_run = get_last_run_timestamp(bucket_name, target_prefix)
        date_filter = get_date_filter(last_run)

        # Initialize variables
        offset = 0
        all_data = []

        # Modify your API call to include date filter
        base_url = "https://opendata.rdw.nl/resource/m9d7-ebf2.json"
        if date_filter:
            url = base_url + date_filter
        else:
            url = base_url
        logger.info("Using API URL: %s", url)

        # Get starting offset from checkpoint
        offset = get_checkpoint(bucket_name, target_prefix)
        logger.info("Starting from offset: %d", offset)

        while True:
            try:
                # Fetch batch of data
                logger.info("Fetching records %d to %d", offset, offset + batch_size)
                batch = fetch_rdw_data(url, offset, batch_size)

                if not batch:  # No more data
                    logger.info("No more records found from API. Total records processed: %d",
                                offset)
                    break

                all_data.extend(batch)
                offset += batch_size

                # Save intermediate results every 100K records
                if len(all_data) % 100000 == 0:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    intermediate_file = f"{target_prefix}rdw_vehicles_batch_{offset}_{timestamp}.parquet"
                    
                    # Convert to DataFrame and directly to parquet bytes
                    df = pl.DataFrame(all_data)
                    buffer = io.BytesIO()
                    df.write_parquet(file=buffer)
                    parquet_bytes = buffer.getvalue()
                    
                    save_to_s3(
                        data=parquet_bytes,
                        bucket_name=bucket_name,
                        object_name=intermediate_file
                    )
                    all_data = []  # Clear memory
                
                    # Save checkpoint only after successful S3 save
                    save_checkpoint(bucket_name, target_prefix, offset)

            except requests.exceptions.RequestException as e:
                logger.error("API request failed at offset %d: %s", offset, str(e))
                raise

        # Save any remaining data
        if all_data:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            final_file = f"{target_prefix}rdw_vehicles_final_{timestamp}.parquet"
            
            # Convert to DataFrame and directly to parquet bytes
            df = pl.DataFrame(all_data)
            buffer = io.BytesIO()
            df.write_parquet(file=buffer)
            parquet_bytes = buffer.getvalue()
            
            save_to_s3(
                data=parquet_bytes,
                bucket_name=bucket_name,
                object_name=final_file
            )
            save_last_run_timestamp(bucket_name, target_prefix)
            
            # Cleanup checkpoint file after successful run
            cleanup_checkpoint(bucket_name, target_prefix)

        # Log completion
        duration = datetime.now() - start_time
        logger.info("Process completed successfully in %s", duration)
        sys.exit(0)

    except Exception as e:
        logger.error("Process failed: %s", str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
