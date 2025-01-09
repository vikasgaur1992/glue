import boto3
import logging
from time import sleep
 
# Setup logging
log_file = 'pausstats.log'
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)
 
# Initialize Glue client
glue_client = boto3.client('glue', region_name='us-east-1')
 
# File containing the list of databases and tables
#input_file = 'all_table_list.txt'
input_file = "/home/ec2-user/alltablesglue/all_table_list.txt"
 
def stop_column_statistics(database_name, table_name):
    """
    Stops the column statistics task schedule for a given database and table.
    """
    try:
        glue_client.stop_column_statistics_task_run_schedule(
            DatabaseName=database_name,
            TableName=table_name
        )
        logging.info(f"Stopped column statistics schedule for {database_name}.{table_name}")
    except glue_client.exceptions.EntityNotFoundException:
        logging.warning(f"Column statistics task schedule not found for {database_name}.{table_name}")
    except Exception as e:
        logging.error(f"Error stopping column statistics for {database_name}.{table_name}: {e}")
 
def process_table_list(file_path):
    """
    Reads the database and table list file and stops column statistics schedules.
    """
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or ',' not in line:
                    logging.warning(f"Skipping invalid line: {line}")
                    continue
 
                database_name, table_name = line.split(',', 1)
                stop_column_statistics(database_name.strip(), table_name.strip())
#                sleep(0.5)  # Throttle requests to avoid API limits
    except FileNotFoundError:
        logging.error(f"Input file not found: {file_path}")
    except IOError as e:
        logging.error(f"Error reading input file {file_path}: {e}")
 
if __name__ == '__main__':
    logging.info("Starting to process table list")
    process_table_list(input_file)
    logging.info("Processing complete")
