import boto3
import random
import os
import shutil
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config
 
# Paths
base_path = "/home/ec2-user/glue_stats_creation/"
backup_path = os.path.join(base_path, "bkp_log/")
migration_file_path = os.path.join(base_path, "missing_glue_stats.txt")
source_file_path = "/home/ec2-user/alltablesg/missing_glue_stats.txt"
log_file_path = os.path.join(base_path, "gluestatlogfile.txt")
 
# Constants
role_arn = "arn:aws:iam::"
catalog_id = ""
 
# Boto3 clients
#lakeformation_client = boto3.client("lakeformation", config=config)
#glue_client = boto3.client("glue", config=config)
 
# Boto3 clients
lakeformation_client = boto3.client("lakeformation")
glue_client = boto3.client("glue")
 
def log(message):
    """Log a message to both the console and the log file."""
    try:
        with open(log_file_path, "a") as log_file:
            log_file.write(f"{datetime.datetime.now()}: {message}\n")
    except IOError as e:
        print(f"Failed to write to log file: {e}")
    print(message)
 
def generate_random_cron():
    """Generate a random cron schedule."""
    minute = random.randint(0, 59)
    hour = random.randint(0, 23)
    day = random.randint(5, 25)
    return f"cron({minute} {hour} {day} * ? *)"
 
def process_entry(database_name, table_name):
    """Process a single database and table entry."""
    try:
        log(f"Processing Database: {database_name}, Table: {table_name}")
 
        # Grant SELECT permission
        lakeformation_client.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": role_arn},
            Resource={"Table": {"DatabaseName": database_name, "Name": table_name}},
            Permissions=["SELECT", "DESCRIBE", "INSERT", "ALTER", "DELETE", "DROP"],
            PermissionsWithGrantOption=["SELECT", "DESCRIBE", "INSERT", "ALTER", "DELETE", "DROP"]
        )
        log(f"Successfully granted SELECT permission for {database_name}.{table_name}")
 
        # Generate random cron schedule
        cron_schedule = generate_random_cron()
 
        # Create column statistics task
        glue_client.create_column_statistics_task_settings(
            DatabaseName=database_name,
            TableName=table_name,
            Role=role_arn,
            Schedule=cron_schedule,
            CatalogID=catalog_id
        )
        log(f"Successfully created Glue column statistics task for {database_name}.{table_name} with schedule {cron_schedule}")
 
    except Exception as e:
        log(f"Failed to process {database_name}.{table_name}: {e}")
 
def backup_and_replace_files():
    """Backup current files and replace the migration file."""
    try:
        if not os.path.exists(backup_path):
            os.makedirs(backup_path)
 
        for file in os.listdir(base_path):
            if file.endswith(".txt"):
                shutil.copy(os.path.join(base_path, file), backup_path)
 
        if os.path.exists(source_file_path):
            shutil.copy(source_file_path, migration_file_path)
            log(f"Replaced {migration_file_path} with {source_file_path}")
        else:
            log(f"Source file {source_file_path} not found!")
            return False
    except IOError as e:
        log(f"File operation failed: {e}")
        return False
 
    return True
 
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
 
    # Backup and replace files
    if not backup_and_replace_files():
        log("File preparation failed. Aborting process.")
        return
 
    # Process migration file
    if not os.path.exists(migration_file_path):
        log(f"Error: File {migration_file_path} not found!")
        return
 
    entries = []
    try:
        with open(migration_file_path, "r") as db_details_file:
            for line in db_details_file:
                try:
                    database_name, table_name = line.strip().split(",")
                    if not database_name or not table_name:
                        log(f"Skipping invalid line: {line.strip()}")
                        continue
                    entries.append((database_name, table_name))
                except ValueError as e:
                    log(f"Skipping invalid line: {line.strip()} - Error: {e}")
    except IOError as e:
        log(f"Error reading {migration_file_path}: {e}")
        return
 
    # Use ThreadPoolExecutor to process entries in parallel
    max_threads = 5  # Adjust this based on your system resources
    with ThreadPoolExecutor(max_threads) as executor:
        futures = [executor.submit(process_entry, db, table) for db, table in entries]
        for future in as_completed(futures):
            # Process results (if needed) or just wait for completion
            pass
 
    log(f"Process completed at {datetime.datetime.now()}")
 
if __name__ == "__main__":
    main()
