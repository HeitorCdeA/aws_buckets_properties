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
    data1, data2 = {}, {}
    try:
        with open('diff_comparison/s3_bucket_metadata.json', 'r') as file:
            data1 = json.load(file)
        with open('diff_comparison/s3_bucket_metadata_2.json', 'r') as file:
            data2 = json.load(file)
    except Exception as e:
        print(f"Error loading JSON files: {e}")
        return

    # Initialize the output dictionary for buckets only
    output = {"Buckets": {}}

    # Process each key in the union of both data sets' keys
    for key in set(data1.keys()).union(data2.keys()):
        bucket_details = {}
        bucket_diff = compare_json(data1.get(key, {}), data2.get(key, {}), key)
        
        # Set the status based on whether there are differences
        bucket_details["Status"] = "OK" if not bucket_diff else "Differences"
        
        # Set the operations; if no differences, set to an empty dictionary
        bucket_details["Operations"] = bucket_diff if bucket_diff else {}

        # Assign the detailed bucket data to the main output under Buckets
        output["Buckets"][key] = bucket_details

    # Write the refined output to a file
    with open('differences_output.json', 'w') as file:
        json.dump(output, file, indent=4, sort_keys=True)  # Maintain insertion order

if __name__ == "__main__":
    main()

