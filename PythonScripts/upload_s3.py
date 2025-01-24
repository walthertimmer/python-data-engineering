import logging
import os
import sys
from datetime import datetime
from typing import Optional, List, Dict, Union
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
import hashlib

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
    logger = logging.getLogger(__name__)
    
    s3_access_key = get_env_var("S3_ACCESS_KEY_ID")
    s3_secret_key = get_env_var("S3_SECRET_ACCESS_KEY")
    s3_endpoint_url = get_env_var("S3_ENDPOINT_URL")
    
    logger.info("Creating S3 client with endpoint: %s", s3_endpoint_url)

    return boto3.client(
        's3',
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        endpoint_url=s3_endpoint_url,
        verify=True,
        use_ssl=True
    )
    
def calculate_file_hash(file_path):
    logger = logging.getLogger(__name__)
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    except IOError as e:
        logger.error("Failed to read file for hash calculation: %s", e)
        sys.exit(1)

def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    s3_client = get_s3_client()
    bucket_name = get_env_var("S3_BUCKET")
    file_path = get_env_var("FILE_PATH", "DummyFiles/dummy.csv")
    object_key = get_env_var("OBJECT_KEY", "test/dummy.csv") # write location in bucket

    logger.info("Uploading file %s to S3 bucket %s with key %s", file_path, bucket_name, object_key)

    try:
        file_hash = calculate_file_hash(file_path)
        extra_args = {
            'ChecksumAlgorithm': 'SHA256',
            'ChecksumSHA256': file_hash
        }
        response = s3_client.upload_file(
            file_path, 
            bucket_name, 
            object_key,
            ExtraArgs=extra_args
        )
        logger.info("File uploaded successfully: %s", response)
        
    except ClientError as e:
        logger.error("Failed to upload file: %s", e)
        sys.exit(1)
        
if __name__ == "__main__":
    main()
