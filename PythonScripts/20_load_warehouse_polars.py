"""Transform raw data to Delta tables on S3 using Polars

This script reads files from S3 and writes them to Delta format using Polars.
Configuration is done via environment variables:
- S3_ACCESS_KEY_ID: S3 access key
- S3_SECRET_ACCESS_KEY: S3 secret key  
- S3_ENDPOINT_URL: S3 endpoint URL
- S3_BUCKET: S3 bucket name
- SOURCE_FOLDER: Folder containing raw data
- TARGET_FOLDER: Folder for Delta tables
- FILE_FORMAT: Input file format (csv/json/parquet)
- SEPARATOR: CSV separator character
"""

import os
import logging
from typing import Optional
import polars as pl
from deltalake import write_deltalake, DeltaTable
import boto3
from dotenv import load_dotenv
from datetime import timedelta
import time

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
        endpoint_url=s3_endpoint_url
    )

def list_s3_files(bucket: str, prefix: str) -> list:
    """List all files in S3 bucket with given prefix"""
    s3_client = get_s3_client()
    files = []
    
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            files.extend([obj['Key'] for obj in page['Contents']])
            
    return files

def read_data(file_path: str, file_format: str, separator: str = ",") -> pl.DataFrame:
    """Read data from S3 into Polars DataFrame"""
    logger = logging.getLogger(__name__)
    logger.info("Reading %s file: %s", file_format, file_path)

    storage_options = {
        "key": get_env_var("S3_ACCESS_KEY_ID"),
        "secret": get_env_var("S3_SECRET_ACCESS_KEY"),
        "endpoint_url": get_env_var("S3_ENDPOINT_URL")
    }

    try:
        if file_format == "csv":
            df = pl.read_csv(
                file_path,
                separator=separator,
                storage_options=storage_options
            )
        elif file_format == "json":
            df = pl.read_json(
                file_path,
                storage_options=storage_options
            )
        elif file_format == "parquet":
            df = pl.read_parquet(
                file_path,
                storage_options=storage_options
            )
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
            
        return df
        
    except Exception as e:
        logger.error("Failed to read file %s: %s", file_path, str(e))
        raise

def optimize_delta_table(table_path: str) -> None:
    """Optimize Delta table to improve performance"""
    logger = logging.getLogger(__name__)
    logger.info("Optimizing Delta table: %s", table_path)
    
    try:
        storage_options = {
            'AWS_ACCESS_KEY_ID': get_env_var("S3_ACCESS_KEY_ID"),
            'AWS_SECRET_ACCESS_KEY': get_env_var("S3_SECRET_ACCESS_KEY"), 
            'AWS_ENDPOINT_URL': get_env_var("S3_ENDPOINT_URL")
        }
        
        # Get Delta table instance
        logger.info("Getting Delta table")
        delta_table = DeltaTable(
            table_uri=table_path,
            storage_options=storage_options
        )
        
        # Log table statistics before optimization
        logger.info("Delta table details:")
        logger.info("- Number of files: %d", len(delta_table.files()))
        logger.info("- Table version: %s", delta_table.version())
        
        # Optimize table
        logger.info("Starting table optimization...")
        start_time = time.time()
        optimize_result = delta_table.optimize.compact(
            target_size=256 # MB
        )
        duration = time.time() - start_time
        logger.info("Optimization completed in %.2f seconds", duration)
        logger.info("Optimization stats: %s", optimize_result)
        
        # Vacuum table
        logger.info("Starting vacuum operation...")
        start_time = time.time()
        vacuum_result = delta_table.vacuum(retention_hours=168)  # Clean up old files
        duration = time.time() - start_time
        logger.info("Vacuum completed in %.2f seconds", duration)
        logger.info("Vacuum stats: %s", vacuum_result)
        
        logger.info("Successfully completed all maintenance operations")
        
    except Exception as e:
        logger.error("Failed to optimize table: %s", str(e))
        raise

def write_to_delta(df: pl.DataFrame, table_path: str) -> None:
    """Write Polars DataFrame to Delta format"""
    logger = logging.getLogger(__name__)
    logger.info("Writing to Delta table: %s", table_path)
    
    try:
        # Log DataFrame statistics
        logger.info(f"Writing DataFrame with {df.shape[0]:,} rows and {df.shape[1]} columns")
        logger.info(f"Estimated memory usage: {df.estimated_size('mb'):.2f} MB")
        
        # Convert Polars DataFrame to Arrow Table
        arrow_table = df.to_arrow()
        
        storage_options = {
            'AWS_ACCESS_KEY_ID': get_env_var("S3_ACCESS_KEY_ID"),
            'AWS_SECRET_ACCESS_KEY': get_env_var("S3_SECRET_ACCESS_KEY"), 
            'AWS_ENDPOINT_URL': get_env_var("S3_ENDPOINT_URL")
        }
        
        # Always use overwrite for staging
        write_deltalake(
            table_path,
            arrow_table,
            mode="overwrite",
            storage_options=storage_options
        )
        logger.info("Successfully wrote to Delta table")
    except Exception as e:
        logger.error("Failed to write Delta table: %s", str(e))
        raise
    
def main():
    """Main function to execute the script"""
    try:
        # Load environment variables
        load_dotenv()
        setup_logging()
        logger = logging.getLogger(__name__)
        
        # Get configuration
        bucket = get_env_var("S3_BUCKET", "datahub")
        source_folder = get_env_var("SOURCE_FOLDER", "raw/dummy/")
        target_folder = get_env_var("TARGET_FOLDER", "polars/dummy/")
        file_format = get_env_var("FILE_FORMAT", "csv")
        separator = get_env_var("SEPARATOR", ",")
        
        # List source files
        source_files = list_s3_files(bucket, source_folder)
        if not source_files:
            logger.warning("No files found to process")
            return
            
        # Process each file
        for file_path in source_files:
            try:
                # Read data
                s3_path = f"s3://{bucket}/{file_path}"
                df = read_data(s3_path, file_format, separator)
                
                # Get table name from path
                target_path = f"s3://{bucket}/{target_folder}"
                
                # Write to Delta
                write_to_delta(df, target_path)
                
            except Exception as e:
                logger.error("Failed to process file %s: %s", file_path, str(e))
                continue
        
        # Optimize Delta tables
        optimize_delta_table(target_path)
         
        logger.info("Processing completed successfully")
        
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
