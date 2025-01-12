import boto3
import datetime
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import BotoCoreError, ClientError
 
# Paths
base_path = "/home/ec2-user/deletecolumnstat/"
backup_path = os.path.join(base_path, "bkp_log/")
#source_file_path = "/home/ec2-user/alltablesg/database_table_columns_list.txt"
source_file_path = "/home/ec2-user/columnname/database_table_columns_list.txt"
log_file_path = os.path.join(base_path, "gluestatlogfile.txt")
 
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
 
def delete_column_statistics_schedule(database_name, table_name):
    """
    Delete the column statistics schedule for the given database and table.
    """
    try:
        log(f"Deleting column statistics schedule for {database_name}.{table_name}")
        glue_client.stop_column_statistics_task_run_schedule(
#            CatalogId=catalog_id,
            DatabaseName=database_name,
            TableName=table_name
        )
        log(f"Successfully deleted column statistics schedule for {database_name}.{table_name}")
    except glue_client.exceptions.EntityNotFoundException:
        log(f"Column statistics schedule not found for {database_name}.{table_name}. Skipping.")
    except (BotoCoreError, ClientError) as e:
        log(f"Error deleting column statistics schedule for {database_name}.{table_name}: {e}")
 
def backup_file(file_path, backup_folder):
    """
    Backup the given file to the backup folder.
    """
    try:
        if not os.path.exists(backup_folder):
            os.makedirs(backup_folder)
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file_path = os.path.join(backup_folder, f"{os.path.basename(file_path)}_{timestamp}.bak")
        shutil.copy(file_path, backup_file_path)
        log(f"Backed up file {file_path} to {backup_file_path}")
    except IOError as e:
        log(f"Failed to backup file {file_path}: {e}")
 
def process_file(file_path, max_threads=20):
    """
    Process the file and delete column statistics schedules for each entry.
    """
    entries = []
    try:
        with open(file_path, "r") as f:
            for line in f:
                try:
                    database_name, table_name, _ = line.strip().split(",")  # Ignore column_name
                    if database_name and table_name:
                        entries.append((database_name, table_name))
                    else:
                        log(f"Skipping invalid line: {line.strip()}")
                except ValueError:
                    log(f"Skipping invalid line: {line.strip()}")
    except IOError as e:
        log(f"Error reading file {file_path}: {e}")
        return
 
    # Process entries with a thread pool
    with ThreadPoolExecutor(max_threads) as executor:
        futures = [executor.submit(delete_column_statistics_schedule, db, table) for db, table in set(entries)]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                log(f"Error in processing entry: {e}")
 
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
 
    # Backup the source file
    backup_file(source_file_path, backup_path)
 
    # Process the file
    if not os.path.exists(source_file_path):
        log(f"Error: File {source_file_path} not found!")
        return
 
    process_file(source_file_path, max_threads=20)  # Adjust max_threads based on system capacity
 
    log(f"Process completed at {datetime.datetime.now()}")
 
if __name__ == "__main__":
    main()
