"""Clean up an S3 folder by deleting all objects under a specified prefix

Usage:
    python cleanup_s3_folder.py 

Environment variables:
    S3_ACCESS_KEY_ID: S3 access key
    S3_SECRET_ACCESS_KEY: S3 secret key
    S3_ENDPOINT_URL: S3 endpoint URL 
    S3_BUCKET: S3 bucket name
    TARGET_PREFIX: Prefix/folder to delete (optional)
"""

import os
import logging
from typing import Optional
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

def setup_logging(log_level: str = "INFO") -> None:
    """Configure logging with timestamp, level and message"""
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def get_env_var(var_name: str, default: Optional[str] = None) -> str:
    """Get environment variable with optional default value"""
    value = os.getenv(var_name, default)
    if value is None:
        raise ValueError(f"Missing required environment variable: {var_name}")
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

def delete_s3_folder(bucket_name: str, prefix: str) -> None:
    """Delete all objects under specified prefix in S3 bucket"""
    logger = logging.getLogger(__name__)
    s3_client = get_s3_client()
    
    try:
        # List all objects under prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        # Track counts
        total_objects = 0
        deleted_objects = 0

        # Delete objects in batches
        for page in page_iterator:
            if 'Contents' not in page:
                logger.info(f"No objects found with prefix '{prefix}'")
                return

            # Prepare objects to delete
            objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
            total_objects += len(objects_to_delete)

            if objects_to_delete:
                response = s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': objects_to_delete}
                )
                
                # Count successful deletions
                if 'Deleted' in response:
                    deleted_objects += len(response['Deleted'])
                
                # Log any errors
                if 'Errors' in response:
                    for error in response['Errors']:
                        logger.error(f"Error deleting {error['Key']}: {error['Message']}")

        logger.info(f"Deleted {deleted_objects}/{total_objects} objects from prefix '{prefix}'")

    except ClientError as e:
        logger.error(f"Failed to delete folder: {str(e)}")
        raise

def main():
    """Main function"""
    try:
        load_dotenv()
        setup_logging()
        logger = logging.getLogger(__name__)
        
        # Get configuration
        bucket_name = get_env_var("S3_BUCKET", "datahub")
        prefix = get_env_var("TARGET_PREFIX", "polars/dummy/")
        
        # Ensure prefix ends with /
        if not prefix.endswith('/'):
            prefix += '/'
            
        logger.info(f"Starting cleanup of s3://{bucket_name}/{prefix}")
        delete_s3_folder(bucket_name, prefix)
        logger.info("Cleanup completed successfully")
        
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()