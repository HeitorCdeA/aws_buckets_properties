import boto3  # Boto3 is the Amazon Web Services (AWS) SDK for Python, which allows Python developers to write software that uses services like Amazon S3 and Amazon EC2.
import os     # Provides a way of using operating system dependent functionality like reading or writing to a file.
import json   # Enables encoding and decoding data in JSON format.
from concurrent.futures import ThreadPoolExecutor  # Allows performing parallel tasks using threads.
import threading  # Provides a way to create and manipulate threads for concurrent execution.
import logging  # Used for logging events that happen when the program runs.
from botocore.exceptions import ClientError, BotoCoreError  # Exception classes for handling errors from Boto3.

filename = "s3_bucket_metadata.json"  # File to store the collected S3 bucket metadata.

# Setup logging to log events into a file with specific formatting.
logging.basicConfig(filename=f'{filename}.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

s3_client = boto3.client('s3')  # Create an S3 client object using Boto3 that will be used to make API calls.
output_mutex = threading.Lock()  # A lock to manage output synchronization among threads.

def list_all_buckets():
    # Function to list all S3 buckets in your AWS account and log the result.
    try:
        buckets = s3_client.list_buckets()['Buckets']
        logging.info(f"Retrieved {len(buckets)} buckets.")
        return buckets
    except Exception as e:
        logging.error(f"Failed to list buckets: {str(e)}")
        return []

def get_bucket_metadata(bucket_name, get_func):
    # Retrieve metadata for a specific bucket using a function name (get_func).
    try:
        func_result = getattr(s3_client, get_func)(Bucket=bucket_name)
        if func_result:
            return (get_func, func_result)
        else:
            logging.warning(f"No data returned by {get_func} for bucket {bucket_name}.")
            return (get_func, {"Error": "No data returned"})
    except (ClientError, BotoCoreError, Exception) as e:
        logging.error(f"Error while fetching {get_func} for bucket {bucket_name}: {e}")
        return (get_func, {"Error": str(e)})

def get_all_bucket_metadata(bucket_name):
    # Collects all available metadata for a given bucket by calling multiple S3 API methods.
    gets = sorted([
        # List of S3 API methods to call for fetching different types of metadata.
        "get_bucket_accelerate_configuration",
        "get_bucket_acl",
        "get_bucket_analytics_configuration",
        "get_bucket_cors",
        "get_bucket_encryption",
        "get_bucket_intelligent-tiering_configuration",
        "get_bucket_inventory_configuration",
        "get_bucket_lifecycle_configuration",
        "get_bucket_location",
        "get_bucket_logging",
        "get_bucket_metrics_configuration",
        "get_bucket_notification_configuration",
        "get_bucket_ownership_controls",
        "get_bucket_policy",
        "get_bucket_policy_status",
        "get_bucket_replication",
        "get_bucket_request_payment",
        "get_bucket_tagging",
        "get_bucket_versioning",
        "get_bucket_website"
    ])
    data = {}
    with ThreadPoolExecutor() as executor:
        # Use a thread pool to execute API calls in parallel.
        results = executor.map(lambda x: get_bucket_metadata(bucket_name, x), gets)
    for result in results:
        data[result[0]] = result[1]
    logging.info(f"Metadata retrieved for bucket {bucket_name}.")
    return {bucket_name: data}

def verify_data(data):
    # Log errors but do not discard data
    errors = {key: val for key, val in data.items() if "Error" in val}
    if errors:
        logging.warning(f"Errors found in fetched data, but will include in output: {errors}")
    # Always return True to ensure no data is discarded
    return True

def stream_json_data(data, filename):
    # Write the JSON data to a file, checking for errors and updating existing data if necessary.
    if not verify_data(data): 
        logging.error("Data verification logged errors, but proceeding with file write.")

    temp_filename = filename + '.tmp'
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as file:
                existing_data = json.load(file)
            existing_data.update(data)
            with open(temp_filename, 'w') as file:
                json.dump(existing_data, file, indent=4, sort_keys=True)
        else:
            with open(temp_filename, 'w') as file:
                json.dump(data, file, indent=4, sort_keys=True)
        os.replace(temp_filename, filename)
        logging.info(f"Data successfully written to {filename}, including entries with errors.")
        return True
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error while writing to file {filename}: {e}")
    except Exception as e:
        logging.error(f"Failed to update file {filename}: {str(e)}")
    return False

def calculate_percentage(current_index, total_count): 
    # Calculate and log the progress of processing the S3 buckets.
    percentage = (current_index + 1) / total_count * 100
    with output_mutex:
        logging.info(f"Progress: {percentage:.2f}% complete.")
        print(f"Progress: {percentage:.2f}% complete.")
    return percentage
    
def sort_json_file(filename):
    # Sorts the JSON file alphabetically by bucket name and logs the action.
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
        sorted_data = {key: data[key] for key in sorted(data)}
        with open(filename, 'w') as file:
            json.dump(sorted_data, file, indent=4, sort_keys=True)
        logging.info(f"JSON file {filename} sorted alphabetically by bucket name.")
    except Exception as e:
        logging.error(f"Failed to sort JSON file {filename}: {str(e)}")


def main():
    # Main function to orchestrate the fetching, processing, and logging of S3 bucket metadata.
    buckets = list_all_buckets()
    total_buckets = len(buckets)
    for index, bucket in enumerate(buckets):
        bucket_name = bucket['Name']
        logging.info(f"Processing bucket {index + 1}/{total_buckets}: {bucket_name}")
        print(f"Processing bucket {index + 1} of {total_buckets}: {bucket_name}")
        metadata = get_all_bucket_metadata(bucket_name)
        if not stream_json_data(metadata, filename):
            logging.error(f"Failed to process data for bucket {bucket_name}.")
        calculate_percentage(index, total_buckets)
    sort_json_file(filename)

if __name__ == "__main__":
    main()
