import boto3
import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import BotoCoreError, ClientError
 
# Paths
base_path = "/home/ec2-user/columnname/"
source_file_path = "/home/ec2-user/columnname/database_table_columns_list.txt1"
log_file_path = os.path.join(base_path, "gluestatlogfile.txt")
processed_file_path = os.path.join(base_path, "processed_columns.txt")  # Tracks completed entries
 
# Constants
catalog_id = ""
 
# Boto3 clients
glue_client = boto3.client("glue")
 
 
def log(message):
    """Log a message to both the console and the log file."""
    try:
        with open(log_file_path, "a") as log_file:
            log_file.write(f"{datetime.datetime.now()}: {message}\n")
    except IOError as e:
        print(f"Failed to write to log file: {e}")
    print(message)
 
 
def delete_column_statistics(database_name, table_name, column_name):
    """Delete column statistics for the given database, table, and column."""
    try:
        glue_client.delete_column_statistics_for_table(
            CatalogId=catalog_id,
            DatabaseName=database_name,
            TableName=table_name,
            ColumnName=column_name
        )
        log(f"Successfully deleted column statistics for {database_name}.{table_name}.{column_name}")
        return f"{database_name},{table_name},{column_name}"
    except glue_client.exceptions.EntityNotFoundException:
        log(f"Column statistics not found for {database_name}.{table_name}.{column_name}. Skipping.")
    except (BotoCoreError, ClientError) as e:
        log(f"Error deleting column statistics for {database_name}.{table_name}.{column_name}: {e}")
    return None
 
 
def process_file(file_path, max_threads=10, batch_size=1000):
    """Process the file and delete column statistics for each entry."""
    processed_entries = set()
 
    # Load already processed entries if checkpointing
    if os.path.exists(processed_file_path):
        with open(processed_file_path, "r") as f:
            processed_entries = set(line.strip() for line in f)
 
    with open(file_path, "r") as file:
        batch = []
        with ThreadPoolExecutor(max_threads) as executor:
            for line in file:
                if line.strip() in processed_entries:
                    continue  # Skip already processed entries
 
                try:
                    database_name, table_name, column_name = line.strip().split(",")
                    batch.append((database_name, table_name, column_name))
                except ValueError:
                    log(f"Skipping invalid line: {line.strip()}")
 
                # Process batch
                if len(batch) >= batch_size:
                    process_batch(batch, executor, processed_entries)
                    batch.clear()
 
            # Process remaining entries in the last batch
            if batch:
                process_batch(batch, executor, processed_entries)
 
 
def process_batch(batch, executor, processed_entries):
    """Process a batch of entries."""
    futures = [executor.submit(delete_column_statistics, db, table, column) for db, table, column in batch]
    for future in as_completed(futures):
        result = future.result()
        if result:
            processed_entries.add(result)
            # Append successfully processed entry to file
            with open(processed_file_path, "a") as processed_file:
                processed_file.write(f"{result}\n")
 
 
def main():
    """Main function to orchestrate the process."""
    # Clear the log file at the start of the process
    try:
        with open(log_file_path, "w"):
            pass
    except IOError as e:
        print(f"Failed to clear log file: {e}")
        return
 
    log(f"Starting process at {datetime.datetime.now()}")
 
    # Process the file
    if not os.path.exists(source_file_path):
        log(f"Error: File {source_file_path} not found!")
        return
 
    process_file(source_file_path, max_threads=10, batch_size=1000)  # Adjust max_threads and batch_size
 
    log(f"Process completed at {datetime.datetime.now()}")
 
 
if __name__ == "__main__":
    main()
