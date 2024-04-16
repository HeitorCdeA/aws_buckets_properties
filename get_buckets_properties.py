import boto3
import json

s3_client = boto3.client('s3')

def list_all_buckets():
    return s3_client.list_buckets()['Buckets']

def get_all_bucket_metadata(bucket_name):
    data = {}
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

    for get_func in gets:
        try:
            func_result = getattr(s3_client, get_func)(Bucket=bucket_name)
            data[get_func] = func_result
        except Exception as e:
            data[get_func] = {"Error": str(e)}

    return {bucket_name: data}

def stream_json_data(data, filename):
    try:
        with open(filename, 'r+') as file:
            existing_data = json.load(file)
            if isinstance(existing_data, dict):
                existing_data.update(data)
            else:
                existing_data = data
            file.seek(0)
            json.dump(existing_data, file, indent=4)
            file.truncate()
    except FileNotFoundError:
        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)
    except json.JSONDecodeError:
        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)

def calculate_percentage(current_index, total_count):
    return (current_index + 1) / total_count * 100

def main():
    buckets = list_all_buckets()
    total_buckets = len(buckets)
    filename = "s3_bucket_metadata.json"
    for index, bucket in enumerate(buckets):
        bucket_name = bucket['Name']
        print(f"Processing bucket {index + 1} of {total_buckets}: {bucket_name}")
        metadata = get_all_bucket_metadata(bucket_name)
        stream_json_data(metadata, filename)
        percentage_complete = calculate_percentage(index, total_buckets)
        print(f"Metadata for {bucket_name} has been added to {filename}. \nProgress: {percentage_complete:.0f}% complete.")

if __name__ == "__main__":
    main()
