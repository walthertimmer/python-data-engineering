"""Transform raw data to warehouse using Dask 

This script reads files from S3 using Dask and writes them to Delta format.
"""

import os
import shutil
import time
import logging
from typing import Optional
import sys
from dotenv import load_dotenv
import boto3
import dask.dataframe as dd
import dask.config
from dask.diagnostics import ProgressBar
from dask.distributed import Client, LocalCluster, performance_report
from deltalake import DeltaTable, write_deltalake
# import pyarrow as pa
# from pyarrow import schema as pa_schema
# from pyarrow import field as pa_field

try:
    load_dotenv()
except Exception as e:
    logging.error("Failed to load .env file: %s", str(e))

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
        verify=True
    )
    
def setup_dask() -> Client:
    """Setup Dask client with optimal configuration"""
    logger = logging.getLogger(__name__)
    logging.info("Setting up Dask client...")
    
    # n_workers = max(1, multiprocessing.cpu_count() - 1)
    n_workers = 4
    
    # Create a 4-process cluster (running locally). Note only one thread
    # per-worker: because polling is per-process, you can't run multiple
    # threads per worker, otherwise you'll get results that combine memory
    # usage of multiple tasks.
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=1,
        memory_limit="2GB",
        )
    client = Client(cluster)
    logger.info("Dask Dashboard URL: %s", client.dashboard_link)
    dask.config.set(
        {"distributed.worker.memory.target": 0.6,  # Spill to disk at 80% memory usage
        "distributed.worker.memory.spill": 0.8,  # Start spilling to disk
        "distributed.worker.memory.pause": 0.90, # Pause execution at 95% memory usage
        "distributed.scheduler.allowed-failures": 3,  # Allow a few task retries
        }
    )
    dask_config = dask.config.config
    logger.info("Dask configuration: %s", dask_config)
    
    return client, cluster

def close_dask(client, cluster):
    """Close Dask client and cluster"""
    logger = logging.getLogger(__name__)
    logger.info("Closing Dask client and cluster...")
    client.close()
    cluster.close()

def read_data(source_path: str,
              file_format: str,
              block_size: str = "100MB") -> dd.DataFrame:
    """Read data from S3 using Dask"""
    logger = logging.getLogger(__name__)
    logger.info("Reading %s files from: %s", file_format, source_path)

    storage_options = {
        'key': get_env_var("S3_ACCESS_KEY_ID"),
        'secret': get_env_var("S3_SECRET_ACCESS_KEY"),
        'client_kwargs': {
            'endpoint_url': get_env_var("S3_ENDPOINT_URL")
        }
    }
    
    start = time.time()
    if file_format == 'csv':
        with ProgressBar():
            df = dd.read_csv(source_path, 
                            assume_missing=True,
                            storage_options=storage_options,
                            blocksize=block_size)
        elapsed = time.time() - start
        logger.info("Data loaded in %.2f seconds", elapsed)
    elif file_format == 'parquet':
        with ProgressBar():
            df = dd.read_parquet(source_path, 
                                assume_missing=True,
                                storage_options=storage_options,
                                blocksize=block_size)
        elapsed = time.time() - start
        logger.info("Data loaded in %.2f seconds", elapsed)
    elif file_format == 'json':
        with ProgressBar():
            df = dd.read_json(source_path, 
                            assume_missing=True,
                            storage_options=storage_options,
                            blocksize=block_size)
        elapsed = time.time() - start
        logger.info("Data loaded in %.2f seconds", elapsed)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    return df

def clean_delta_table(target_path: str) -> None:
    """Clean Delta table by removing all files"""
    logger = logging.getLogger(__name__)
    logger.info("Cleaning Delta table at: %s", target_path)
    
    try:
        delta_table = DeltaTable(
            target_path,
            storage_options = {
                'AWS_ACCESS_KEY_ID': get_env_var("S3_ACCESS_KEY_ID"),
                'AWS_SECRET_ACCESS_KEY': get_env_var("S3_SECRET_ACCESS_KEY"),
                'AWS_ENDPOINT_URL': get_env_var("S3_ENDPOINT_URL")
            }
        )
        delta_table.delete()
        logger.info("Delta table cleaned successfully.")
    except Exception as e:
        logger.error("Failed to clean Delta table: %s", str(e))
        raise

def write_to_delta(df: dd.DataFrame,
                   target_path: str,
                   client,
                   cluster,
                   partition_size: str = "256MB"
                   ) -> None:
    """Write Dask DataFrame to Delta format with disk spillover support
    
    Args:
        df: Dask DataFrame to write
        target_path: S3 path where to write the Delta table
        partition_size: Size of each partition to process
    """
    logger = logging.getLogger(__name__)
    temp_dir = "/tmp/dask-delta-tmp"
    os.makedirs(temp_dir, exist_ok=True)
        
    clean_delta_table(target_path)
    
    try:
        logger.info("Repartitioning Dask DataFrame to %s chunks...", partition_size)
        # Repartition to manageable chunks
        df = df.repartition(partition_size=partition_size)
        
        # Get total partitions for progress tracking
        total_partitions = df.npartitions
        
        # Process each partition
        for i, partition in enumerate(df.partitions):
            try:
                logger.info("Processing partition %d/%d", i+1, total_partitions)
                
                # Compute single partition
                logger.info("Computing partition %d", i+1)
                pandas_chunk = partition.compute()
                
                # Write mode: overwrite for first chunk, append for rest
                write_mode = "overwrite" if i == 0 else "append"
                
                logger.info("Writing partition %d to Delta format", i+1)
                write_deltalake(
                    target_path,
                    pandas_chunk,
                    mode=write_mode,
                    storage_options={
                    'AWS_ACCESS_KEY_ID': get_env_var("S3_ACCESS_KEY_ID"),
                    'AWS_SECRET_ACCESS_KEY': get_env_var("S3_SECRET_ACCESS_KEY"), 
                    'AWS_ENDPOINT_URL': get_env_var("S3_ENDPOINT_URL")
                    }
                )
                logger.info("Successfully wrote partition %d", i+1)
                
            except Exception as e:
                logger.error("Error processing partition %d: %s", i, str(e))
                raise
            
        logger.info("Successfully wrote all data to Delta format")
        
    except Exception as e:
        logger.error("Failed to write to Delta format: %s", str(e))
        close_dask(client, cluster)
        raise
    finally:
        # Cleanup temporary files
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

def get_worker_memory_stats(client):
    """Get memory statistics from all workers"""
    try:
        workers = client.scheduler_info()['workers']
        if not workers:
            logging.warning("No workers found in cluster")
            return 0, 0
        memory_usage = []
        for worker in workers.values():
            try:
                # Try different memory attributes
                if 'memory' in worker:
                    mem = worker['memory']
                elif 'memory_info' in worker:
                    mem = worker['memory_info']['used']
                elif 'metrics' in worker and 'memory' in worker['metrics']:
                    mem = worker['metrics']['memory']
                else:
                    mem = worker.get('memory_limit', 0)  # fallback to memory limit
                memory_usage.append(mem)
            except Exception as we:
                logging.warning(f"Failed to get memory for worker: {str(we)}")
                memory_usage.append(0)
                
        if not memory_usage:
            return 0, 0
            
        return max(memory_usage) / 1e9, sum(memory_usage) / 1e9
    except Exception as e:
        logging.warning(f"Failed to get memory stats: {str(e)}, type: {type(e)}")
        return 0, 0

def main():
    """Main function to execute the script"""
    setup_logging()
    client = None
    cluster = None
    
    try:
        # Get configuration
        source_path = get_env_var("SOURCE_PATH", "raw/dummy/*")
        target_path = get_env_var("TARGET_PATH", "dask/dummy/")
        file_format = get_env_var("FILE_FORMAT", "csv").lower()
        bucket_name = get_env_var("S3_BUCKET")

        source_path = f"s3://{bucket_name}/{source_path}"
        target_path = f"s3://{bucket_name}/{target_path}"
        
        client, cluster = setup_dask()
        
        # Initial memory usage
        initial_max_mem, initial_total_mem = get_worker_memory_stats(client)
        logging.info("Initial memory usage - Max: %.2f GB, Total: %.2f GB", initial_max_mem, initial_total_mem)
        
        # Add performance monitoring
        with performance_report(filename="dask-report.html"):
            # Read data
            df = read_data(source_path, file_format)
            
            # Write to Delta format
            write_to_delta(df=df,
                        target_path=target_path,
                        client=client,
                        cluster=cluster)
            
            # Final memory usage
            final_max_mem, final_total_mem = get_worker_memory_stats(client)
            logging.info("Final memory usage - Max: %.2f GB, Total: %.2f GB", final_max_mem, final_total_mem)
            logging.info("Memory increase - Max: %.2f GB", (final_max_mem - initial_max_mem))
            
            # Log task completion statistics
            scheduler_info = client.scheduler_info()
            logging.info("Total tasks processed: %d", scheduler_info.get('total', 0))
            logging.info("Tasks completed: %d", scheduler_info.get('completed', 0))
                
        close_dask(client, cluster)
                
        logging.info("Process completed successfully.")
        
    except Exception as e:
        logging.error("Error: %s", str(e))
        sys.exit(1)
    finally:
        if client is not None:
            try:
                client.close()
                if cluster is not None:
                    cluster.close()
                logging.info("Dask client and cluster closed successfully")
            except Exception as e:
                logging.error("Error closing Dask client/cluster: %s", str(e))
    
if __name__ == "__main__":
    main()
