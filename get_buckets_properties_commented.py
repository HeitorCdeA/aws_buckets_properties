import boto3
import json

# Initialize the Amazon S3 client with default credentials set up through the AWS CLI or AWS SDK configuration
s3_client = boto3.client('s3')

def list_all_buckets():
    """
    This function retrieves a list of all buckets in your AWS account and returns them.
    """
    return s3_client.list_buckets()['Buckets']

def get_all_bucket_metadata(bucket_name):
    """
    Retrieves and compiles various configuration and metadata for a specified S3 bucket.
    Each API call corresponds to a different type of configuration data.
    """
    data = {}  # Dictionary to store all metadata fetched from different API calls
    # List of all API methods we want to call to fetch bucket information
    gets = [
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
    ]

    # Iterate over each function in the list, call it with the bucket name, and store the result
    for get_func in gets:
        try:
            # Dynamically call the function by name, pass the bucket name, and store the response
            func_result = getattr(s3_client, get_func)(Bucket=bucket_name)
            data[get_func] = func_result
        except Exception as e:
            # If an API call fails, store the error message instead of the result
            data[get_func] = {"Error": str(e)}

    # Return the data dictionary nested under the bucket name
    return {bucket_name: data}

def stream_json_data(data, filename):
    """
    Writes or updates JSON data in a specified file. It ensures the data structure remains a valid JSON.
    Handles file not found errors by creating a new file, and data corruption by rewriting the corrupt file.
    """
    try:
        # Try to open the existing JSON file in read/write mode
        with open(filename, 'r+') as file:
            # Load the current data and update it with the new data
            existing_data = json.load(file)
            if isinstance(existing_data, dict):
                existing_data.update(data)
            else:
                existing_data = data
            # Write back the updated data to the file and truncate any excess content
            file.seek(0)
            json.dump(existing_data, file, indent=4)
            file.truncate()
    except FileNotFoundError:
        # If the file doesn't exist, open in write mode and dump the data
        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)
    except json.JSONDecodeError:
        # If the file is corrupt (not valid JSON), overwrite it with the new data
        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)

def calculate_percentage(current_index, total_count):
    """
    Computes the percentage of completion based on the current item index and the total number of items.
    """
    return (current_index + 1) / total_count * 100

def main():
    """
    Main function to execute the workflow: listing buckets, fetching their metadata, 
    storing/updating the metadata in a JSON file, and displaying the progress.
    """
    buckets = list_all_buckets()  # Get all buckets
    total_buckets = len(buckets)  # Total number of buckets
    filename = "s3_bucket_metadata.json"  # JSON file to store metadata
    for index, bucket in enumerate(buckets):
        bucket_name = bucket['Name']
        print(f"Processing bucket {index + 1} of {total_buckets}: {bucket_name}")
        metadata = get_all_bucket_metadata(bucket_name)  # Get metadata for the bucket
        stream_json_data(metadata, filename)  # Update the JSON file with this metadata
        percentage_complete = calculate_percentage(index, total_buckets)  # Calculate the progress
        print(f"Metadata for {bucket_name} has been added to {filename}. \nProgress: {percentage_complete:.0f}% complete.")

if __name__ == "__main__":
    main()