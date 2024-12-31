"""Delta table maintenance script for S3

This script finds all Delta tables in an S3 bucket and performs maintenance:
- VACUUM to clean up old files
- OPTIMIZE to compact small files
"""

import os
import logging
from typing import Optional
import sys
from dotenv import load_dotenv
import boto3
from pyspark.sql import SparkSession
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

def get_java_home():
    """Get Java home directory for Linux and macOS"""
    
    def validate_java_path(path):
        """Helper to validate Java installation path"""
        if not os.path.exists(path):
            logging.debug(f"Path does not exist: {path}")
            return False
            
        java_bin = os.path.join(path, "bin", "java")
        if not os.path.exists(java_bin):
            logging.debug(f"Java binary not found at: {java_bin}")
            return False
            
        logging.debug(f"Valid Java installation found at: {path}")
        return True

    java_paths = [
        '/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home',  # Homebrew Java 17, brew install openjdk@17
        '/usr/lib/jvm/java-17-openjdk-amd64',
        '/usr/lib/jvm/java-17-openjdk',
        '/usr/lib/jvm/java-17-openjdk-arm64',
        '/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home',  # Homebrew path
        '/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home',
        '/Library/Java/JavaVirtualMachines/openjdk-17.jdk/Contents/Home',
        '/usr/lib/jvm/java-17-openjdk-amd64',
        '/usr/lib/jvm/java-17-openjdk',
        '/usr/java/default',
        '/usr/lib/jvm/default-java',
    ]
            
    # check common paths
    for path in java_paths:
        if validate_java_path(path):
            return path
    
    # Finally check JAVA_HOME env var
    java_home = os.getenv('JAVA_HOME')
    if java_home and validate_java_path(java_home):
        return java_home
    
    raise RuntimeError("Could not find valid JAVA_HOME path")

def get_ivy_dir():
    """Determine appropriate Ivy directory based on environment"""
    # Check if running in k8s
    if os.path.exists('/var/run/secrets/kubernetes.io'):
        default_ivy_dir = '/tmp/.ivy2'
    else:
        # Local development (Mac)
        default_ivy_dir = os.path.expanduser('~/.ivy2')
    
    # Allow override via environment variable
    ivy_dir = os.getenv('SPARK_IVY_DIR', default_ivy_dir)
    
    # Ensure directory exists
    os.makedirs(ivy_dir, exist_ok=True)
    return ivy_dir

def init_spark_session():
    """Initialize Spark session with Delta support"""
    # Set Java home with error handling
    try:
        os.environ['JAVA_HOME'] = get_java_home()
        print(f"Using JAVA_HOME: {os.environ['JAVA_HOME']}")
    except Exception as e:
        logging.error(f"Failed to set JAVA_HOME: {str(e)}")
        sys.exit(1)
    
    try:
        logging.info("Initializing Spark session")
        
        ivy_dir = get_ivy_dir()
        logging.info(f"Using Ivy directory: {ivy_dir}")
        
        spark = (SparkSession.builder
            .master("local[*]")
            .appName("DeltaMaintenance")
            # Basic Delta Lake configs
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # Core packages only
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.3.4," + # needed for S3
                    "io.delta:delta-spark_2.12:3.2.0," + # make sure to use delta-spark and not delta-core
                    "org.apache.spark:spark-sql_2.12:3.3.0")
            # Add Ivy configs
            .config("spark.jars.ivy", ivy_dir)
            .config("spark.driver.extraJavaOptions", f"-Divy.home={ivy_dir}")
            # add S3 credentials
            .config("spark.hadoop.fs.s3a.access.key", get_env_var("S3_ACCESS_KEY_ID"))
            .config("spark.hadoop.fs.s3a.secret.key", get_env_var("S3_SECRET_ACCESS_KEY"))
            .config("spark.hadoop.fs.s3a.endpoint", get_env_var("S3_ENDPOINT_URL"))
            # Basic configs
            .config("spark.sql.debug.maxToStringFields", 100)
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        logging.info(f"Done with init Spark session")
    except Exception as e:
        logging.error(f"Failed to initialize Spark session: {str(e)}")
        raise
    return spark

def maintain_delta_table(spark: SparkSession, table_path: str, vacuum_hours: int = 168) -> None:
    """Perform maintenance operations on a Delta table
    
    Args:
        spark: SparkSession
        table_path: Full path to Delta table
        vacuum_hours: Hours to retain history (default 7 days)
    """
    try:
        logging.info(f"Starting maintenance for table: {table_path}")
        
        # Load the table
        delta_table = DeltaTable.forPath(spark, table_path)
        
        # OPTIMIZE table
        logging.info(f"Running OPTIMIZE on {table_path}")
        delta_table.optimize().executeCompaction()
        
        # VACUUM table (default 7 days retention)
        logging.info(f"Running VACUUM on {table_path} (retaining {vacuum_hours} hours of history)")
        delta_table.vacuum(retentionHours=vacuum_hours)
        
        logging.info(f"Completed maintenance for table: {table_path}")
        
    except Exception as e:
        logging.error(f"Error maintaining table {table_path}: {str(e)}")
        raise

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

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Get configuration
        bucket_name = get_env_var("S3_BUCKET", "datahub")
        prefix = get_env_var("DELTA_PREFIX", "warehouse/")
        vacuum_hours = int(get_env_var("VACUUM_HOURS", "168"))
        
        # Initialize Spark
        spark = init_spark_session()
        
        # Get list of Delta tables
        tables = list_delta_tables(bucket_name, prefix)
        logging.info(f"Found {len(tables)} Delta tables")
        
        # Perform maintenance on each table
        for table_path in tables:
            maintain_delta_table(spark, table_path, vacuum_hours)
            
        logging.info("Maintenance completed successfully")
        
    except Exception as e:
        logging.error(f"Maintenance failed: {str(e)}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()
