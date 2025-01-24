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
from datetime import datetime
from typing import Optional, List, Dict, Union
import time
import requests
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
from dateutil.relativedelta import relativedelta
import polars as pl
import hashlib

try:
    load_dotenv()
except Exception as e:
    print("Failed to load .env file: %s", {str(e)})

logging.basicConfig(
    level="INFO",
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.info("Init logging with level: %s", logger.level)

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
    logger.debug("Creating S3 client with endpoint: %s", s3_endpoint_url)
    return boto3.client(
        's3',
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        endpoint_url=s3_endpoint_url,
        verify=True,
        use_ssl=True
    )

def get_last_run_timestamp(bucket_name, prefix):
    """Get timestamp of last successful run from S3"""
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
    timestamp = datetime.now().strftime("%Y-%m-%d")
    local_timestamp_file = "/tmp/last_run_timestamp.txt"
    logger.info("Saving last run timestamp to S3: %s", timestamp)
    try:
        with open(local_timestamp_file, 'w') as f:
            f.write(timestamp)
        save_to_s3(
            file_path=local_timestamp_file,
            bucket_name=bucket_name,
            object_key=f"{prefix}last_run_timestamp.txt"
        )
        logger.info("Timestamp saved successfully to s3")
    except Exception as e:
        logger.error("Failed to save last_run_timestamp to S3: %s", str(e))
        raise
    finally:
        if os.path.exists(local_timestamp_file):
            logger.info("Cleaning up local timestamp file")
            os.remove(local_timestamp_file)

def get_date_filter(last_run: str) -> str:
    """Create date filter with 1 month overlap to catch delayed entries"""
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

def calculate_file_hash(file_path):
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    except IOError as e:
        logger.error("Failed to read file for hash calculation: %s", e)
        sys.exit(1)

def save_to_s3(
    file_path: str, 
    bucket_name: str, 
    object_key: str) -> None:
    """Upload a file to S3 using direct file transfer"""
    try:
        s3_client = get_s3_client()
        file_hash = calculate_file_hash(file_path)
        extra_args = {
            'ChecksumAlgorithm': 'SHA256',
            'ChecksumSHA256': file_hash
        }
        logger.info("Uploading to S3")
        logger.debug("Bucket_name: %s", bucket_name)
        logger.debug("File path: %s", file_path)
        logger.debug("Object_key: %s", object_key)
        logger.debug("Extra_args: %s", extra_args)
        
        response = s3_client.upload_file(
            file_path, 
            bucket_name, 
            object_key,
            ExtraArgs=extra_args
        )
        logger.info("File uploaded successfully: %s", response)
    except Exception as e:
        logger.error(f"Failed to upload to S3: {str(e)}")
        raise

def save_checkpoint(bucket_name: str, prefix: str, offset: int) -> None:
    """Save current processing offset to S3"""
    logger.info("Saving checkpoint to S3 at offset %d", offset)  
    local_checkpoint_file = "/tmp/checkpoint.txt"
    checkpoint_data = f"{offset}\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    try:
        logger.info("Writing checkpoint data locally: %s", checkpoint_data)
        with open(local_checkpoint_file, 'w') as f:
                f.write(checkpoint_data)   
        save_to_s3(
            file_path=local_checkpoint_file,
            bucket_name=bucket_name,
            object_key=f"{prefix}checkpoint.txt"
        ) 
        logger.info("Saving checkpoint to S3 successful")
    except Exception as e:
        logger.error("Failed to save checkpoint to S3: %s", str(e))
        raise
    finally:
        if os.path.exists(local_checkpoint_file):
            logger.info("Cleaning up local checkpoint file")
            os.remove(local_checkpoint_file)

def get_checkpoint(bucket_name: str, prefix: str) -> int:
    """Get last saved offset from checkpoint"""
    try:
        checkpoint_file = f"{prefix}checkpoint.txt"
        s3_client = get_s3_client()
        logger.info("Checking for checkpoint file at %s", checkpoint_file)
        response = s3_client.get_object(Bucket=bucket_name, Key=checkpoint_file)
        checkpoint_data = response['Body'].read().decode('utf-8')
        # Get first line containing offset
        offset = int(checkpoint_data.split('\n')[0])
        logger.info("Checkpoint found at offset: %d", offset)
        return offset
    except s3_client.exceptions.NoSuchKey:
        logger.info("No checkpoint found, starting from beginning")
        return 0
    except Exception as e:
        logger.error("Error reading checkpoint: %s", str(e))
        return 0
    
def cleanup_checkpoint(bucket_name: str, prefix: str) -> None:
    """Delete checkpoint file after successful run"""
    checkpoint_file = f"{prefix}checkpoint.txt"
    try:
        logger.info("Cleaning up Checkpoint file at %s", checkpoint_file)
        s3_client = get_s3_client()
        s3_client.delete_object(Bucket=bucket_name, Key=checkpoint_file)
        logger.info("Checkpoint file cleaned up successfully")
    except Exception as e:
        logger.warning("Failed to cleanup checkpoint file: %s", str(e))

def get_configuration() -> tuple[str, str, int, int]:
    """Get configuration parameters."""
    bucket_name = get_env_var("S3_BUCKET", "datahub")
    target_prefix = get_env_var("TARGET_LOCATION", "raw/rdw-vehicles/")
    batch_size = 10000
    save_per_x = 100000
    logger.info("Using config: %s, %s, %d, %d", bucket_name, target_prefix, batch_size, save_per_x)
    return bucket_name, target_prefix, batch_size, save_per_x

def construct_api_url(bucket_name: str, target_prefix: str) -> str:
    """Construct API URL with date filter if applicable."""
    base_url = "https://opendata.rdw.nl/resource/m9d7-ebf2.json"
    last_run = get_last_run_timestamp(bucket_name, target_prefix)
    date_filter = get_date_filter(last_run)
    api_url = base_url + date_filter if date_filter else base_url
    logger.info("Using API URL: %s", api_url)
    return api_url

def process_data(
    bucket_name: str, 
    offset: int, 
    batch_size: int, 
    url: str, 
    save_per_x: int, 
    target_prefix: str) -> None:
    """Process data"""
    all_data = []
    batch_start_offset = offset
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
            # Save intermediate results every x records
            if len(all_data) >= save_per_x and len(all_data) % save_per_x == 0:
                logging.info("Saving intermediate results")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                intermediate_file = f"{target_prefix}rdw_vehicles_batch_from_{batch_start_offset}_to_{offset}_{timestamp}.parquet"
                local_file = f"/tmp/temp.parquet"
                
                try: 
                    # Convert to DataFrame and to parquet
                    df = pl.DataFrame(all_data)
                    df.write_parquet(file=local_file)
                    
                    # Upload directly from file
                    save_to_s3(
                        file_path=local_file,
                        bucket_name=bucket_name,
                        object_key=intermediate_file
                    )
                    
                    # Save checkpoint only after successful S3 save
                    save_checkpoint(bucket_name, target_prefix, offset)
                    batch_start_offset = offset
                    all_data = []  # Clear memory
                except Exception as e:
                    logger.error("Failed to save intermediate results: %s", str(e))
                    raise                
                finally:
                    # Cleanup local file after successful run
                    if os.path.exists(local_file):
                        logger.info("Cleaning up intermediate local file")
                        os.remove(local_file)
        except requests.exceptions.RequestException as e:
            logger.error("API request failed at offset %d: %s", offset, str(e))
            raise

    # Save any remaining data
    if all_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        final_file = f"{target_prefix}rdw_vehicles_final_from_{batch_start_offset}_to_{offset}_{timestamp}.parquet"
        local_file = f"/tmp/temp.parquet"
        logger.info("Saving final results")
        try:
            # Convert to DataFrame and to parquet
            df = pl.DataFrame(all_data)
            df.write_parquet(file=local_file)
            
            save_to_s3(
                file_path=local_file,
                bucket_name=bucket_name,
                object_key=final_file
            )
            save_last_run_timestamp(bucket_name, target_prefix)
            all_data = []  # Clear memory
            
            # Cleanup checkpoint file after successful run
            cleanup_checkpoint(bucket_name, target_prefix)
        except Exception as e:
            logger.error("Failed to save final results: %s", str(e))
            raise
        finally:
            # Cleanup local file after successful run
            if os.path.exists(local_file):
                logger.info("Cleaning up final local file")
                os.remove(local_file)
               
    logger.info("All data processed successfully")

def main() -> None:
    """Main function to extract RDW data and save to S3"""
    try:
        start_time = datetime.now()
        bucket_name, target_prefix, batch_size, save_per_x = get_configuration()      
        url = construct_api_url(bucket_name, target_prefix)
        offset = get_checkpoint(bucket_name, target_prefix)
        process_data(bucket_name, offset, batch_size, url, save_per_x, target_prefix)
        duration = datetime.now() - start_time
        logger.info("Process completed successfully in %s", duration)
        sys.exit(0)
    except Exception as e:
        logger.error("Process failed: %s", str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
