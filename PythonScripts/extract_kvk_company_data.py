"""Extract data from KVK Open Dataset.

Get KVK Data. Weekly refreshed set. Anonymized company data. 

Resources:
- KVK Open Dataset: https://www.kvk.nl/producten-bestellen/kvk-handelsregister-open-data-set/
- Dataset Portal: https://data.overheid.nl/dataset/kvk-hr-open-data-set
- Direct Download: https://static.kvk.nl/download/kvk-open-data-set-handelsregister.zip

"""

import logging
import os
import sys
from datetime import datetime
from typing import Optional
import zipfile
import requests
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
load_dotenv()

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

def download_zip_file(url: str, file_path: str) -> None:
    """Download zip file from URL to file path"""
    response = requests.get(url)
    with open(file_path, 'wb') as f:
        f.write(response.content)

def extract_zip_file(zip_file: str, extract_dir: str) -> None:
    """Extract zip file to directory"""
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

def get_s3_client():
    """Create S3 client with custom endpoint"""
    aws_access_key = get_env_var("AWS_ACCESS_KEY_ID")
    aws_secret_key = get_env_var("AWS_SECRET_ACCESS_KEY")
    endpoint_url = get_env_var("S3_ENDPOINT_URL")
    
    return boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        endpoint_url=endpoint_url,
        verify=True  # SSL verification
    )

def save_to_s3(file_path: str, bucket_name: str, object_name: str) -> None:
    """Save file to S3 bucket"""
    try:        
        logger = logging.getLogger(__name__)
        
        # Create S3 client with custom endpoint
        s3_client = get_s3_client()    
        
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Get file size for logging
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # Convert to MB
        logger.info(f"Starting upload of {file_path} ({file_size:.2f} MB) to s3://{bucket_name}/{object_name}")
        
        # Upload file
        s3_client.upload_file(file_path, bucket_name, object_name)
        logger.info(f"Successfully uploaded {file_path} to s3://{bucket_name}/{object_name}")
        
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Failed to upload to S3: {str(e)}")
        raise

def main() -> None:
    """Main function"""
    try:
        # Setup logging
        setup_logging()
        logger = logging.getLogger(__name__)
        
        # Log script start
        logger.info("Starting process")
        start_time = datetime.now()        
        
        # Main logic
        logger.info("Downloading zip")
        download_zip_file(url="https://static.kvk.nl/download/kvk-open-data-set-handelsregister.zip", 
                          file_path="kvk-open-data-set-handelsregister.zip")

        logger.info("Extracting zip")
        extract_zip_file(zip_file="kvk-open-data-set-handelsregister.zip", 
                         extract_dir="kvk-open-data-set-handelsregister")

        logger.info("Uploading to S3")
        save_to_s3(
            file_path="kvk-open-data-set-handelsregister/kvk-open-data-set-handelsregister.csv",
            bucket_name="datahub",
            object_name="bronze/kvk/kvk-open-data-set-handelsregister-company.csv"
        )

        # Log completion
        duration = datetime.now() - start_time
        logger.info(f"Process completed successfully in {duration}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
