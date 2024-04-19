import json

def compare_json(data1, data2, path=""):
    differences = {}
    if isinstance(data1, dict) and isinstance(data2, dict):
        keys1, keys2 = set(data1.keys()), set(data2.keys())
        common_keys = keys1 & keys2
        added = keys2 - keys1
        removed = keys1 - keys2

        # Handle items only in one dictionary
        for key in added:
            new_path = f"{path}/{key}" if path else key
            differences[new_path] = f"Added in updated JSON: {data2[key]}"
        for key in removed:
            new_path = f"{path}/{key}" if path else key
            differences[new_path] = f"Removed from original JSON: {data1[key]}"

        # Recursively compare common keys
        for key in common_keys:
            new_path = f"{path}/{key}" if path else key
            result = compare_json(data1[key], data2[key], new_path)
            if result:
                differences.update(result)

    elif isinstance(data1, list) and isinstance(data2, list):
        len1, len2 = len(data1), len(data2)
        common_length = min(len1, len2)
        for index in range(common_length):
            new_path = f"{path}[{index}]"
            result = compare_json(data1[index], data2[index], new_path)
            if result:
                differences.update(result)
        if len1 > len2:
            differences[path] = f"Extra items in original JSON from index {common_length}: {data1[common_length:]}"
        elif len2 > len1:
            differences[path] = f"Extra items in updated JSON from index {common_length}: {data2[common_length:]}"

    else:
        if data1 != data2:
            differences[path] = f"Original: {data1}, Updated: {data2}"

    return differences if differences else None

def main():
    # Load JSON data from files
    with open('s3_bucket_metadata.json', 'r') as file:
        data1 = json.load(file)

    with open('s3_bucket_metadata_2.json', 'r') as file:
        data2 = json.load(file)

    # Check for matching bucket names
    buckets1 = set(data1.keys())
    buckets2 = set(data2.keys())
    common_buckets = buckets1.intersection(buckets2)
    missing_from_1 = buckets2 - buckets1
    missing_from_2 = buckets1 - buckets2

    if missing_from_1 or missing_from_2:
        print("Mismatch in bucket names found:")
        print("Buckets only in first file:", missing_from_2)
        print("Buckets only in second file:", missing_from_1)

    # Proceed with content comparison for common buckets
    diff = {bucket: compare_json(data1[bucket], data2[bucket], bucket) for bucket in common_buckets}

    output = {}
    for bucket, changes in diff.items():
        if changes:
            output[bucket] = changes
        else:
            output[bucket] = "OK"

    # Handle buckets without matches
    for bucket in missing_from_2:
        output[bucket] = "Missing in updated JSON"
    for bucket in missing_from_1:
        output[bucket] = "Missing in original JSON"

    with open('differences_output_9.json', 'w') as file:
        json.dump(output, file, indent=4, sort_keys=True)

if __name__ == "__main__":
    main()
