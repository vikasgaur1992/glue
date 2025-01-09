import boto3
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import os
import tempfile
from time import sleep
 
# Setup logging
log_file = '/home/ec2-user/alltablesg/script_log.txt'
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    #level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)
 
# Initialize Glue client
glue_client = boto3.client('glue', region_name='us-east-1')
 
# File paths
base_path = "/home/ec2-user/alltablesg"
if not os.path.exists(base_path) or not os.access(base_path, os.W_OK):
    logging.warning(f"Base path unavailable or not writable: {base_path}. Using temporary directory.")
    base_path = tempfile.mkdtemp()
 
output_file = f"{base_path}/missing_glue_stats.txt"
existing_file = f"{base_path}/existing_glue_stats.txt"
all_tables_file = f"{base_path}/all_table_list.txt"
 
# Retry decorator
def retry(exception_to_check, tries=3, delay=2):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(tries):
                try:
                    return func(*args, **kwargs)
                except exception_to_check as e:
                    logging.warning(f"Retry {attempt + 1}/{tries} for {func.__name__}: {e}")
                    sleep(delay)
            logging.error(f"Operation {func.__name__} failed after {tries} retries.")
            raise
        return wrapper
    return decorator
 
# Backup previous files with a timestamp
@retry(Exception)
def backup_files(file_paths):
    for file_path in file_paths:
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            if content:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_file = f"{file_path}.{timestamp}.bak"
                with open(backup_file, 'w') as f:
                    f.write(content)
        except FileNotFoundError:
            logging.info(f"File not found for backup: {file_path}")
        except IOError as e:
            logging.error(f"Error backing up file {file_path}: {e}")
            raise
 
# Initialize and clear output files
@retry(Exception)
def initialize_files(file_paths):
    for file_path in file_paths:
        try:
            open(file_path, 'w').close()
        except IOError as e:
            logging.error(f"Error initializing file {file_path}: {e}")
            raise
 
def fetch_databases():
    """Fetch all databases from Glue with pagination."""
    logging.info("Fetching list of databases...")
    all_databases = []
    next_token = None
 
    try:
        while True:
            response = glue_client.get_databases(NextToken=next_token) if next_token else glue_client.get_databases()
            databases = [db['Name'] for db in response.get('DatabaseList', [])]
            all_databases.extend(databases)
            logging.info(f"Fetched {len(databases)} databases in this page.")
            next_token = response.get('NextToken')
            if not next_token:
                break
        logging.info(f"Total databases fetched: {len(all_databases)}")
    except Exception as e:
        logging.error(f"Error fetching databases: {e}")
    return all_databases
 
def fetch_tables(database_name):
    """Fetch all tables for a given database with pagination."""
    logging.info(f"Fetching tables for database: {database_name}")
    all_tables = []
    next_token = None
 
    try:
        while True:
            response = glue_client.get_tables(DatabaseName=database_name, NextToken=next_token) if next_token else glue_client.get_tables(DatabaseName=database_name)
            tables = [table['Name'] for table in response.get('TableList', [])]
            all_tables.extend(tables)
            next_token = response.get('NextToken')
            if not next_token:
                break
        logging.info(f"Total tables fetched for {database_name}: {len(all_tables)}")
    except Exception as e:
        logging.error(f"Error fetching tables for {database_name}: {e}")
    return all_tables
 
def check_column_statistics(database_name, table_name):
    """Check if column statistics schedule exists for the given table."""
    try:
        response = glue_client.get_column_statistics_task_settings(DatabaseName=database_name, TableName=table_name)
        if 'SCHEDULED' in response['ColumnStatisticsTaskSettings']['Schedule']['State']:
            logging.info(f"Column statistics schedule exists for {database_name}.{table_name}")
            return database_name, table_name, 'existing'
        else:
            logging.info(f"No column statistics schedule for {database_name}.{table_name}")
            return database_name, table_name, 'missing'
    except glue_client.exceptions.EntityNotFoundException:
        logging.warning(f"No column statistics task settings found for {database_name}.{table_name}")
        return database_name, table_name, 'missing'
    except Exception as e:
        logging.error(f"Error checking column stats for {database_name}.{table_name}: {e}")
        return database_name, table_name, 'missing'
 
def process_databases():
    """Process databases to get tables and check column statistics."""
    databases = fetch_databases()
 
    if not databases:
        logging.error("No databases found. Exiting.")
        return
 
    with ThreadPoolExecutor(max_workers=10) as executor:
        table_futures = {executor.submit(fetch_tables, db): db for db in databases}
        tables_per_db = {}
 
        for future in as_completed(table_futures):
            db_name = table_futures[future]
            try:
                tables_per_db[db_name] = future.result()
            except Exception as e:
                logging.error(f"Error fetching tables for database {db_name}: {e}")
 
        try:
            with open(all_tables_file, 'w') as f:
                for db_name, tables in tables_per_db.items():
                    for table in tables:
                        f.write(f"{db_name},{table}\n")
        except IOError as e:
            logging.error(f"Error writing to all_tables_file: {e}")
 
        column_futures = []
        for db_name, tables in tables_per_db.items():
            for table in tables:
                column_futures.append(executor.submit(check_column_statistics, db_name, table))
 
        try:
            with open(output_file, 'a') as missing_file, open(existing_file, 'a') as existing_file1:
                for future in as_completed(column_futures):
                    try:
                        db_name, table_name, status = future.result()
                        if status == 'existing':
                            existing_file1.write(f"{db_name},{table_name}\n")
                        else:
                            missing_file.write(f"{db_name},{table_name}\n")
                    except Exception as e:
                        logging.error(f"Error processing column statistics: {e}")
        except IOError as e:
            logging.error(f"Error writing to output or existing files: {e}")
 
    logging.info("Script execution completed.")
 
if __name__ == "__main__":
    logging.info("Starting process")
    backup_files([output_file, existing_file, all_tables_file, log_file])
    initialize_files([output_file, existing_file, all_tables_file])
    process_databases()
    logging.info("Process completed")
