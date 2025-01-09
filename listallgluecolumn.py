import boto3
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
 
# Setup logging
log_file = 'fetch_columns.log'
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)
 
# Initialize Glue client
glue_client = boto3.client('glue', region_name='us-east-1')
 
# File to store the results
output_file = 'database_table_columns_list.txt'
 
def fetch_columns(database_name, table_name):
    """
    Fetch column names for a given database and table and log them.
    """
    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        columns = response['Table']['StorageDescriptor']['Columns']
        column_names = [col['Name'] for col in columns]
 
        with open(output_file, 'a') as f:
            for column_name in column_names:
                f.write(f"{database_name},{table_name},{column_name}\n")
 
        logging.info(f"Fetched {len(column_names)} columns for {database_name}.{table_name}")
    except boto3.exceptions.Boto3Error as e:
        logging.error(f"Error fetching columns for {database_name}.{table_name}: {e}")
 
def fetch_tables_and_columns(database_name):
    """
    Fetch tables for a database and initiate fetching columns for each table.
    """
    logging.info(f"Fetching tables for database: {database_name}")
    next_token = None
    all_tables = []
 
    try:
        while True:
            response = glue_client.get_tables(DatabaseName=database_name, NextToken=next_token) if next_token else glue_client.get_tables(DatabaseName=database_name)
            tables = [table['Name'] for table in response.get('TableList', [])]
            all_tables.extend(tables)
            next_token = response.get('NextToken')
            if not next_token:
                break
 
        logging.info(f"Total tables fetched for {database_name}: {len(all_tables)}")
        return all_tables
    except boto3.exceptions.Boto3Error as e:
        logging.error(f"Error fetching tables for database {database_name}: {e}")
        return []
 
def process_database(database_name, max_workers=10):
    """
    Fetch tables and columns for a database using multithreading.
    """
    tables = fetch_tables_and_columns(database_name)
 
    if not tables:
        logging.warning(f"No tables found for database {database_name}. Skipping.")
        return
 
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_columns, database_name, table): table
            for table in tables
        }
 
        for future in as_completed(futures):
            table_name = futures[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error processing table {database_name}.{table_name}: {e}")
 
def fetch_all_databases_and_columns(max_workers=5):
    """
    Fetch all databases and initiate processing for each database.
    """
    logging.info("Fetching list of databases...")
    next_token = None
    databases = []
 
    try:
        while True:
            response = glue_client.get_databases(NextToken=next_token) if next_token else glue_client.get_databases()
            db_list = [db['Name'] for db in response.get('DatabaseList', [])]
            databases.extend(db_list)
            next_token = response.get('NextToken')
            if not next_token:
                break
 
        logging.info(f"Total databases fetched: {len(databases)}")
    except boto3.exceptions.Boto3Error as e:
        logging.error(f"Error fetching databases: {e}")
        return
 
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_database, database): database
            for database in databases
        }
 
        for future in as_completed(futures):
            database_name = futures[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error processing database {database_name}: {e}")
 
if __name__ == '__main__':
    logging.info("Starting the process to fetch databases, tables, and columns")
    # Initialize the output file
    with open(output_file, 'w') as f:
        f.write("DatabaseName,TableName,ColumnName\n")
    fetch_all_databases_and_columns(max_workers=5)  # Adjust max_workers based on system capacity
    logging.info("Process completed")
