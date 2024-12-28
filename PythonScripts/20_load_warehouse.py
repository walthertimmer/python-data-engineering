"""Transform bronze data to  delta table on S3 
"""

import os
import logging
from typing import Optional
import subprocess
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import SparkContext

try:
    load_dotenv()
except Exception as e:
    print(f"Failed to load .env file: {str(e)}")
    pass

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

def check_bucket_exists(bucket_name: str) -> bool:
    """Check if the S3 bucket exists"""
    s3_client = get_s3_client()
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logging.info(f"Bucket {bucket_name} exists.")
        return True
    except ClientError as e:
        logging.error(f"Bucket {bucket_name} does not exist or you do not have access: {e}")
        return False

def list_s3_objects(bucket_name, prefix):
    """List files in folder on S3"""
    s3_client = get_s3_client()
    logger = logging.getLogger(__name__)
    
    logger.info(f"Listing objects in bucket '{bucket_name}' with prefix '{prefix}'")
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name,Prefix=prefix, Delimiter='/') 
        logger.info(f"Response: {response}")
    except ClientError as e:
        logger.error(f"An error occurred: {e}")
        return []
    
    if 'Contents' in response:
        for obj in response['Contents']:
            print(obj['Key'])
    else:
        print("No files found")
    
    if 'Contents' not in response:
        logger.warning(f"No objects found with prefix '{prefix}' in bucket '{bucket_name}'")
        return []
    
    return [content['Key'] for content in response['Contents']]

def check_java():
    """Check if Java is installed and configured"""
    try:
        subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT)
        return True
    except:
        logging.error("Java is not installed or JAVA_HOME is not set properly")
        return False

def init_spark_session():
    """Initialize Spark session with Delta support"""
    if not check_java():
        raise RuntimeError("Java is required to run Spark")
    
    spark = (SparkSession.builder
        .master("local[*]")  # Run in local mode
        .appName("ETL")
        # Add Delta Lake configs
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Add S3A configs
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Specify compatible versions
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4," +
                "io.delta:delta-spark_2.12:3.2.0," + # make sure to use delta-spark and not delta-core
                "org.apache.spark:spark-sql_2.12:3.3.0," +
                "org.apache.spark:spark-hive_2.12:3.3.0," +
                "org.apache.spark:spark-hadoop-cloud_2.12:3.3.0")
        # add S3 credentials
        .config("spark.hadoop.fs.s3a.access.key", get_env_var("S3_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", get_env_var("S3_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", get_env_var("S3_ENDPOINT_URL"))
        # other config
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        # Security configurations
        .config("spark.driver.allowMultipleContexts", "true")
        .config("spark.security.credentials.hadoop.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .getOrCreate())
    return spark

def clean_table_name(table_name, bronze_prefix):
    """Make sure table name is valid"""
    cleaned_table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')
    cleaned_table_name = cleaned_table_name.replace('_csv', '') \
                                           .replace(bronze_prefix, "") \
                                           .replace("/", "_") \
                                           .replace("-", "_") \
                                           .replace(".csv", "") \
                                           .replace(".json", "") \
                                           .replace(".parquet", "") \
                                           .lower()
    logging.info(f"Cleaned table name: {table_name} to {cleaned_table_name}")
    return cleaned_table_name

def create_table(spark, table_name, df,silver_prefix, bronze_prefix):
    """Create a table using PySpark"""
    cleaned_table_name = clean_table_name(table_name,bronze_prefix)    
    
    # Sanitize column names - replace any invalid characters
    # Clean up column names to only contain alphanumeric chars and underscores
    for column in df.columns:
        clean_name = ''.join(c if c.isalnum() else '_' for c in column)
        clean_name = clean_name.replace('__', '_').replace('___', '_')
        df = df.withColumnRenamed(column, clean_name)
        
    # Convert string columns to proper types
    for field in df.schema.fields:
        if field.dataType.typeName() == "string":
            df = df.withColumn(field.name, df[field.name].cast("string"))
        
    # Define the table path in S3
    silver_path = f"s3a://{get_env_var('S3_BUCKET')}/{silver_prefix}/{cleaned_table_name}"
    
    df.show(5)
    
    # Create table
    try:
        logging.info(f"Creating table: {cleaned_table_name} at {silver_path}")
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(silver_path)
            
        # Create the table metadata if you want to query it using SQL
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {cleaned_table_name}
            USING DELTA
            LOCATION '{silver_path}'
        """)
    except Exception as e:
        logging.error(f"Failed to create table {cleaned_table_name}: {str(e)}")
        raise
    return

def process_and_create_tables(bucket_name, bronze_prefix, silver_prefix):
    """Process files from bronze folder and create tables in silver folder"""
    if not check_bucket_exists(bucket_name):
        logging.error(f"Bucket {bucket_name} does not exist. Exiting the process.")
        return
    
    files = list_s3_objects(bucket_name, bronze_prefix)
    
    if not files:
        logging.info("No files found in the bronze folder. Exiting the process.")
        return
    
    # Initialize Spark session
    spark = init_spark_session()
    
    # debug spark
    spark_context = SparkContext._gateway.jvm.java.lang.System.getProperty("java.class.path")
    logging.info(f"Spark context: {spark_context}")
    
    for file_key in files:
        file_path = f"s3a://{bucket_name}/{file_key}"
        
        # Read CSV file into DataFrame
        df = spark.read.csv(file_path, header=True, inferSchema=True, sep=";")
        df.printSchema()
    
        # Create table name from file path
        table_name = file_key.replace(bronze_prefix, "") \
                             .replace("/", "_") \
                             .replace("-", "_") \
                             .replace(".csv", "")
        logging.info(f"Corrected table name: {table_name}")
        
        create_table(spark, table_name, df, silver_prefix, bronze_prefix)
        
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    bucket_name = get_env_var("S3_BUCKET")
    bronze_prefix = "datahub/bronze/dummy/"
    # bronze_prefix = "datahub/bronze/kvk/"
    silver_prefix = "warehouse"
        
    process_and_create_tables(bucket_name, bronze_prefix, silver_prefix)
    