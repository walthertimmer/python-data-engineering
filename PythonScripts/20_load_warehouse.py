"""Transform bronze data to  delta table on S3 
"""

import os
import logging
from typing import Optional
# import platform
# import subprocess
import sys
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
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
        logging.info(f"Initializing Spark session")
        
        ivy_dir = get_ivy_dir()
        logging.info(f"Using Ivy directory: {ivy_dir}")
                
        spark = (SparkSession.builder
            .master("local[*]")  # Run in local mode (single container)
            .appName("ETL")
            # Basic Delta Lake configs
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # Security
            # .config("spark.hadoop.security.authentication", "simple")
            # .config("spark.security.credentials.enabled", "false")
            # .config("spark.hadoop.security.authorization", "false")
            # .config("spark.driver.extraJavaOptions", "-Djava.security.krb5.conf=/dev/null -Djavax.security.auth.useSubjectCredsOnly=false")
            # .config("spark.executor.extraJavaOptions", "-Djava.security.krb5.conf=/dev/null -Djavax.security.auth.useSubjectCredsOnly=false")
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
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.debug.maxToStringFields", 100)  # Default is 25            
            .getOrCreate())
        
        spark.sparkContext.setLogLevel("INFO")
        logging.info(f"Done with init Spark session")
    except Exception as e:
        logging.error(f"Failed to initialize Spark session: {str(e)}")
        raise

    return spark
            
def clean_table_name(table_name):
    """Make sure table name is valid"""
    logging.info(f"Cleaning table name: {table_name}")
    
    # First get only the filename after the second last /
    cleaned_table_name = table_name.split('/')[-2]
    
    cleaned_table_name = ''.join(e for e in cleaned_table_name if e.isalnum() or e == '_')
    cleaned_table_name = cleaned_table_name.replace('_csv', '') \
                                           .replace("/", "_") \
                                           .replace("-", "_") \
                                           .replace(".csv", "") \
                                           .replace(".json", "") \
                                           .replace(".parquet", "") \
                                           .lower()
    logging.info(f"Cleaned table name: {table_name} to {cleaned_table_name}")
    return cleaned_table_name

def create_table(spark, table_name, df,target_folder):
    """Create a table using PySpark"""
    logging.info(f"Creating table: {table_name}")
    
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
    target_path = f"s3a://{get_env_var('S3_BUCKET')}/{target_folder}/{table_name}"
    
    df.show(5)
    
    # Create table
    try:
        logging.info(f"Creating table: {table_name} at {target_path}")
        df.write \
          .format("delta") \
          .mode("overwrite") \
          .save(target_path)
            
        # Create the table metadata if you want to query it using SQL
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING DELTA
            LOCATION '{target_path}'
        """)
        logging.info(f"Done with creating table: {table_name} at {target_path}")
    except Exception as e:
        logging.error(f"Failed to create table {table_name}: {str(e)}")
        raise
    return

def process_and_create_tables(
    bucket_name,
    source_folder,
    target_folder,
    file_format,
    separator = ";"):
    """Process files from bronze folder and create tables in silver folder"""
    
    if not check_bucket_exists(bucket_name):
        logging.error(f"Bucket {bucket_name} does not exist. Exiting the process.")
        return

    files = list_s3_objects(bucket_name, source_folder)

    if not files:
        logging.error("No files found in the source folder. Exiting the process.")
        return
    
    ### Initialize Spark session
    spark = init_spark_session()
    
    ### debug spark context
    # spark_context = SparkContext._gateway.jvm.java.lang.System.getProperty("java.class.path")
    # logging.info(f"Spark context: {spark_context}")
            
    # read and write entire folder using wildcard
    file_path = f"s3a://{bucket_name}/{source_folder}*.{file_format}"
    
    try:
        # Read all CSV files into a single DataFrame
        if file_format == "csv":
            logging.info(f"Reading files from: {file_path}")
            df = spark.read.csv(file_path,
                            header=True,
                            inferSchema=True,
                            sep=separator)
        else:
            logging.error(f"Unsupported file format: {file_format}")
            return
        df.printSchema()
        
        # Create table name from source folder
        table_name = clean_table_name(source_folder)
        
        create_table(spark, table_name, df, target_folder)
        
    except Exception as e:
        logging.error(f"Error reading files from {file_path}: {str(e)}")
        raise
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    bucket_name = get_env_var("S3_BUCKET", "datahub") # S3 bucket name
    source_folder = get_env_var("SOURCE_FOLDER", "raw/dummy/") # folder that contains raw data, should end with /
    target_folder = get_env_var("TARGET_FOLDER","warehouse") # main folder where delta table will be created
    file_format = get_env_var("FILE_FORMAT","csv") # file format to read
    separator = get_env_var("SEPARATOR",";") # separator for csv files
        
    process_and_create_tables(
        bucket_name=bucket_name,
        source_folder=source_folder,
        target_folder=target_folder,
        file_format=file_format,
        separator=separator
        )

    logging.info(f"Done with processing")
    sys.exit(0)