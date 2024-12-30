# test local spark session directly on container
import os
import logging
from typing import Optional
import subprocess
import sys
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark import SparkContext

def get_java_home():
    """Get Java home directory for Linux container"""
    logging.basicConfig(level=logging.DEBUG)
    
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

    # Common Linux Java paths
    java_paths = [
        '/usr/lib/jvm/java-17-openjdk-amd64',
        '/usr/lib/jvm/java-17-openjdk',
        '/usr/lib/jvm/java-17-openjdk-arm64',
        '/usr/java/default',
        '/usr/lib/jvm/default-java'
    ]
    
    # First check environment variable
    java_home = os.getenv('JAVA_HOME')
    if java_home and validate_java_path(java_home):
        return java_home.rstrip('/bin/java')
        
    # Then check common paths
    for path in java_paths:
        if validate_java_path(path):
            return path
    
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

# Set Java home with error handling
try:
    os.environ['JAVA_HOME'] = get_java_home()
except Exception as e:
    logging.error(f"Failed to set JAVA_HOME: {str(e)}")
    sys.exit(1)

# Configure Ivy directory
ivy_dir = get_ivy_dir()

spark = (SparkSession.builder
            .master("local[*]")  # Run in local mode
            .appName("ETL")
            # Basic Delta Lake configs
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # Core packages only
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.3.4," + # needed for S3
                    "io.delta:delta-spark_2.12:3.2.0," + # make sure to use delta-spark and not delta-core
                    "org.apache.spark:spark-sql_2.12:3.3.0")
            # add S3 credentials
            # .config("spark.hadoop.fs.s3a.access.key", get_env_var("S3_ACCESS_KEY_ID"))
            # .config("spark.hadoop.fs.s3a.secret.key", get_env_var("S3_SECRET_ACCESS_KEY"))
            # .config("spark.hadoop.fs.s3a.endpoint", get_env_var("S3_ENDPOINT_URL"))
            # Basic configs
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.debug.maxToStringFields", 100)  # Default is 25             
            .getOrCreate())

# security 
            # .config("spark.hadoop.security.authentication", "simple")
            # .config("spark.hadoop.security.authorization", "false")
            # .config("spark.security.credentials.enabled", "false")
            # Add Ivy configs
            # .config("spark.jars.ivy", ivy_dir)
            # .config("spark.driver.extraJavaOptions", f"-Divy.home={ivy_dir}")
# Specify compatible versions
            # .config("spark.jars.packages",
            #         "org.apache.hadoop:hadoop-aws:3.3.4," +
            #         "io.delta:delta-spark_2.12:3.2.0," + # make sure to use delta-spark and not delta-core
            #         "org.apache.spark:spark-sql_2.12:3.3.0," +
            #         "org.apache.spark:spark-hive_2.12:3.3.0," +
            #         "org.apache.spark:spark-hadoop-cloud_2.12:3.3.0")
# add S3 credentials
            # .config("spark.hadoop.fs.s3a.access.key", get_env_var("S3_ACCESS_KEY_ID"))
            # .config("spark.hadoop.fs.s3a.secret.key", get_env_var("S3_SECRET_ACCESS_KEY"))
            # .config("spark.hadoop.fs.s3a.endpoint", get_env_var("S3_ENDPOINT_URL"))
            # other config
            # .config("spark.hadoop.fs.s3a.path.style.access", "true")
# Disable security manager
            # .config("spark.driver.extraJavaOptions", "-Djava.security.manager=disallow")
            # .config("spark.executor.extraJavaOptions", "-Djava.security.manager=disallow")