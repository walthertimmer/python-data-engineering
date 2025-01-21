"""Transform raw data to Delta tables on S3 using Polars"""

import os
import sys
import logging
import io
from typing import Optional
import time
from datetime import timedelta
from deltalake import write_deltalake, DeltaTable
import boto3
import polars as pl
from dotenv import load_dotenv
import psutil

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

def get_dynamic_schema(
    s3_client,
    bucket,
    first_file_path,
    file_format: str,
    separator: str = None):
    """Infer schema from first file with increased inference length"""
    logger = logging.getLogger(__name__)
    logger.info("Getting schema for %s file %s", file_format, first_file_path)

    try:
        # Get file from S3
        response = s3_client.get_object(Bucket=bucket, Key=first_file_path)
        if file_format == "json":
            json_data = response['Body'].read()
            df = pl.read_json(
                io.BytesIO(json_data),
                infer_schema_length=100000  # rows to sample
            )
        elif file_format == "csv":
            csv_data = response['Body'].read()
            df = pl.read_csv(
                io.BytesIO(csv_data),
                separator=separator,
                infer_schema_length=100000,
                n_rows=100000  # limit rows for schema inference
            )
        elif file_format == "parquet":
            # Parquet files already contain schema information
            # We can still read a subset of rows to verify
            parquet_data = response['Body'].read()
            df = pl.read_parquet(
                io.BytesIO(parquet_data),
                n_rows=100000
            )
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
            
        logger.info("Schema inferred for format %s: %s", file_format, df.schema)
        return df.schema
    
    except Exception as e:
        logger.error("Failed to infer schema: %s", str(e))
        raise

def read_data(
    file_path: str,
    file_format: str,
    separator: str = None,
    schema: dict = None) -> pl.DataFrame:
    """Read data from S3 into Polars DataFrame"""
    logger = logging.getLogger(__name__)
    logger.info("Reading %s file: %s", file_format, file_path)

    storage_options = {
        "key": get_env_var("S3_ACCESS_KEY_ID"),
        "secret": get_env_var("S3_SECRET_ACCESS_KEY"),
        "endpoint_url": get_env_var("S3_ENDPOINT_URL")
    }
    
    # Check if file is accessible
    s3_client = get_s3_client()
    
    # Extract bucket and key from s3 path
    bucket = file_path.replace("s3://", "").split("/")[0]
    key = "/".join(file_path.replace("s3://", "").split("/")[1:])
    
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except Exception as e:
        logger.error("File not accessible: %s", str(e))
        raise FileNotFoundError(f'File not accessible in S3: {file_path}') from e
    
    logger.info("S3 connection validated, attempting to read file")

    try:
        # https://docs.pola.rs/api/python/stable/reference/api/polars.read_csv.html
        if file_format == "csv":
            df = pl.read_csv(
                file_path,
                separator=separator,
                storage_options=storage_options
            )
        elif file_format == "json":
            # https://docs.pola.rs/api/python/stable/reference/api/polars.read_json.html
            response = s3_client.get_object(Bucket=bucket, Key=key)
            json_data = response['Body'].read()

            # Load JSON data into a Polars DataFrame
            df = pl.read_json(
                io.BytesIO(json_data),
                schema=schema,
                )
            
            # Log memory usage
            process = psutil.Process()
            memory_info = process.memory_info()
            logger.info("Memory usage after reading chunk: %.2f MB", memory_info.rss / 1024 / 1024)
            
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
        logger.info("Writing DataFrame with %s rows and %s columns", f"{df.shape[0]:,}", df.shape[1])
        logger.info("Estimated memory usage: %.2f MB", df.estimated_size('mb'))
        
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
        source_folder = get_env_var("SOURCE_FOLDER", "raw/dummy-json/")
        target_folder = get_env_var("TARGET_FOLDER", "polars/dummy-json/")
        file_format = get_env_var("FILE_FORMAT", "json")
        separator = get_env_var("SEPARATOR", ",")
        
        # List source files
        source_files = list_s3_files(bucket, source_folder)
        if not source_files:
            logger.warning("No files found to process")
            return
        # remove files with different file format
        source_files = [file for file in source_files if file.endswith(file_format)]
        
        # Get schema from first file
        first_file = source_files[0]
        schema = get_dynamic_schema(get_s3_client(), bucket, first_file, file_format, separator)
        logger.info("Inferred schema: %s", schema)
        
        # Initialize empty list to store dataframes
        dfs = []
        failed_files = []
        
        # Read all files
        logger.info("Reading %d files...", len(source_files))
        total_rows = 0
        for file_path in source_files:
            try:
                s3_path = f"s3://{bucket}/{file_path}"
                df = read_data(s3_path, file_format, separator, schema=schema)
                dfs.append(df)
                total_rows += len(df)
                logger.info("Successfully read %s with total %i rows", file_path, total_rows)
            except Exception as e:
                logger.error("Failed to read %s: %s", file_path, str(e))
                failed_files.append(file_path)
                continue
        
        if not dfs:
            logger.error("No files were successfully read")
            sys.exit(1)
            
        # Combine all dataframes
        logger.info("Combining dataframes...")
        combined_df = pl.concat(dfs)
        logger.info("Combined dataframe has %d rows", len(combined_df))
        
        # Write combined data to delta
        target_path = f"s3://{bucket}/{target_folder}"
        write_to_delta(combined_df, target_path)
        
        # Optimize final Delta table
        optimize_delta_table(target_path)
        
        if failed_files:
            logger.warning("Failed to process %d files: %s", len(failed_files), failed_files)
         
        logger.info("Processing completed successfully")
        
    except Exception as e:
        logger.error("Fatal error in main process: %s", str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
