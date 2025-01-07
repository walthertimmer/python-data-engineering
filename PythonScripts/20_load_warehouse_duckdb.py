"""Use DUCKDB to transform data from RAW to warehouse
"""
import logging
from pathlib import Path
import os
import sys
from typing import Optional, List, Dict, Any
import boto3
from botocore.exceptions import ClientError
import duckdb
from dotenv import load_dotenv
import psutil

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
        verify=True  # SSL verification
    )

def install_duckdb_extensions(con) -> None:
    """Install DUCKDB extensions"""
    logger = logging.getLogger(__name__)
    logger.info("Installing DUCKDB extensions")
    
    extensions = [
        'httpfs',
        'aws',
        'json',
        'parquet',
        'sqlite'    
    ]
    for extension in extensions:
        con.execute(f"INSTALL '{extension}';")
        con.execute(f"LOAD '{extension}';")
    return None

def setup_duckdb_s3(con) -> None:
    """Setup DUCKDB with S3 credentials"""
    logger = logging.getLogger(__name__)
    logger.info("Setting up DUCKDB with S3 credentials")
    
    con.sql(f"SET s3_access_key_id='{get_env_var('S3_ACCESS_KEY_ID')}'")
    con.sql(f"SET s3_secret_access_key='{get_env_var('S3_SECRET_ACCESS_KEY')}'")
    s3_endpoint_url = get_env_var('S3_ENDPOINT_URL').replace("https://", "").replace("http://", "")
    con.sql(f"SET s3_endpoint='{ s3_endpoint_url}'")
    con.sql("SET s3_url_style='path'")
    return None

def setup_duckdb_connection() -> Any:
    """Setup DUCKDB connection with S3-synced metadata"""
    logger = logging.getLogger(__name__)
    logger.info("Setting up DUCKDB connection")
        
    # Initialize connection with memory settings
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA memory_limit='3GB'")  # Set memory limit
    con.execute("PRAGMA temp_directory='/tmp/duckdb_temp'")
    con.execute("PRAGMA threads=4")
    
    # Create temp directory if it doesn't exist
    os.makedirs("/tmp/duckdb_temp", exist_ok=True)
    
    logger.info("DuckDB connection initialized with memory limits")
    return con

def get_memory_usage() -> float:
    """Return current memory usage percentage"""
    return psutil.Process().memory_percent()

def cleanup_temp_files() -> None:
    """Cleanup temporary DuckDB files"""
    temp_dir = Path("/tmp/duckdb_temp")
    if temp_dir.exists():
        for file in temp_dir.glob("*"):
            try:
                file.unlink()
            except Exception:
                pass

def get_file_type(file_path):
    """Determine file type from path"""
    if file_path.lower().endswith('.csv'):
        return 'csv'
    elif file_path.lower().endswith('.json'):
        return 'json'
    else:
        raise ValueError(f"Unsupported file type for {file_path}")

def process_files(con: duckdb.DuckDBPyConnection, 
                  source_path: str, 
                  target_path: str) -> None:
    """Process files from RAW to warehouse"""
    logger = logging.getLogger(__name__)
    
    # List all files
    files_query = f"SELECT * FROM glob('{source_path}')"
    source_files = con.execute(files_query).fetchall()
    
    for file_tuple in source_files:
        logger.info("Processing file: %s", file_tuple)
        source_file = file_tuple[0]
        base_name = Path(source_file).stem
        
        # Special handling for S3 paths
        if target_path.startswith('s3://'):
            # Ensure proper S3 URL format
            s3_prefix = 's3://'
            # Keep the full path structure without removing components
            bucket_path = target_path[len(s3_prefix):].rstrip('/')
            file_target = f"{s3_prefix}{bucket_path}/{base_name}.parquet"
        else:
            # Use Path for local files
            file_target = str(Path(target_path).parent / f"{base_name}.parquet")
    
        # get type
        file_type = get_file_type(source_file)
        
        try:
            # Process in chunks
            if file_type == 'csv':
                transform_query = f"""
                    COPY (
                        SELECT DISTINCT * 
                        FROM read_csv_auto('{source_file}')
                    ) TO '{file_target}'
                        (
                        FORMAT 'parquet',
                        COMPRESSION 'ZSTD',
                        OVERWRITE TRUE,
                        ROW_GROUP_SIZE 100000
                        )
                """
            elif file_type == 'json':
                transform_query = f"""
                    COPY (
                        SELECT DISTINCT * 
                        FROM read_json_auto('{source_file}')
                    ) TO '{file_target}'
                        (
                        FORMAT 'parquet',
                        COMPRESSION 'ZSTD',
                        OVERWRITE TRUE,
                        ROW_GROUP_SIZE 100000
                        )
                """
            logger.info(transform_query)
            
            # Check memory before processing
            if get_memory_usage() > 80:
                logger.warning("High memory usage detected: %s%%", get_memory_usage())
                con.execute("CHECKPOINT")  # Force memory cleanup
            
            logger.info("Processing %s to %s", source_file, file_target)
            con.execute("BEGIN TRANSACTION;")
            con.execute(transform_query)
            con.execute("COMMIT;")
            logger.info("Processing done")
            
            # Force garbage collection after each file
            con.execute("PRAGMA memory_limit='3GB'")
            con.execute("PRAGMA force_checkpoint")
        except Exception as e:
            con.execute("ROLLBACK;")
            logger.error("Error processing file %s: %s", source_file, e)
            continue  # Continue with next file instead of failing completely
        finally:
            cleanup_temp_files()

def main():
    """Main function"""
    setup_logging(log_level="INFO")
    logger = logging.getLogger(__name__)
    logger.info("Starting RDW data extraction")
    con = None
    
    # Get environment variables
    bucket_name = get_env_var("S3_BUCKET_NAME", "datahub")
    source_folder = get_env_var("SOURCE_FOLDER", "raw/dummy/*")
    target_folder = get_env_var("TARGET_FOLDER", "duckdb/dummy/")
    source_path = f"s3://{bucket_name}/{source_folder}"
    target_path = f"s3://{bucket_name}/{target_folder}"
    
    try:
        con = setup_duckdb_connection()
        install_duckdb_extensions(con)
        setup_duckdb_s3(con)
        process_files(con, source_path, target_path)
    except Exception as e:
        logger.error("Error in main process: %s", {str(e)})
        raise
    finally:
        if con:
            try:
                con.close()
            except Exception as e:
                logger.error("Error closing connection: %s",{str(e)})
        cleanup_temp_files()
    logger.info("RDW data extraction completed successfully")

if __name__ == "__main__":
    main()
