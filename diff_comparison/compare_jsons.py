import json

def compare_json(data1, data2, path=""):
    # Initialize a dictionary to hold differences between data1 and data2
    differences = {}
    
    # Check if both inputs are dictionaries
    if isinstance(data1, dict) and isinstance(data2, dict):
        # Extract the keys from both dictionaries
        keys1, keys2 = set(data1.keys()), set(data2.keys())
        common_keys = keys1 & keys2  # Keys present in both dictionaries
        added = keys2 - keys1  # Keys not in the first dictionary
        removed = keys1 - keys2  # Keys not in the second dictionary

        # Record added and removed keys with their corresponding values
        for key in added:
            new_path = f"{path}/{key}" if path else key
            differences[new_path] = f"Added in updated JSON: {data2[key]}"
        for key in removed:
            new_path = f"{path}/{key}" if path else key
            differences[new_path] = f"Removed from original JSON: {data1[key]}"

        # Recursively call compare_json for keys present in both dictionaries
        for key in common_keys:
            new_path = f"{path}/{key}" if path else key
            result = compare_json(data1[key], data2[key], new_path)
            if result:
                differences.update(result)

    # Check if both inputs are lists
    elif isinstance(data1, list) and isinstance(data2, list):
        len1, len2 = len(data1), len(data2)
        common_length = min(len1, len2)  # Number of elements to compare
        for index in range(common_length):
            new_path = f"{path}[{index}]"
            result = compare_json(data1[index], data2[index], new_path)
            if result:
                differences.update(result)
        # Record discrepancies in list size
        if len1 > len2:
            differences[path] = f"Extra items in original JSON from index {common_length}: {data1[common_length:]}"
        elif len2 > len1:
            differences[path] = f"Extra items in updated JSON from index {common_length}: {data2[common_length:]}"

    # Handle non-container types: compare directly
    else:
        if data1 != data2:
            differences[path] = f"Original: {data1}, Updated: {data2}"

    # Return the differences found, or None if there are no differences
    return differences if differences else None

def main():
    # Dictionaries to store the data from JSON files
    data1, data2 = {}, {}
    try:
        # Load the first JSON file
        with open('diff_comparison/s3_bucket_metadata.json', 'r') as file:
            data1 = json.load(file)
        # Load the second JSON file
        with open('diff_comparison/s3_bucket_metadata_2.json', 'r') as file:
            data2 = json.load(file)
    except Exception as e:
        # Print an error message if the files cannot be loaded
        print(f"Error loading JSON files: {e}")
        return

    # Dictionary to store the comparison results
    output = {"Buckets": {}}

    # Compare each key (bucket) in the combined set of keys from both files
    for key in set(data1.keys()).union(data2.keys()):
        bucket_details = {}
        bucket_diff = compare_json(data1.get(key, {}), data2.get(key, {}), key)
        bucket_details["Status"] = "OK" if not bucket_diff else "Differences"
        bucket_details["Operations"] = bucket_diff if bucket_diff else {}
        output["Buckets"][key] = bucket_details

    # Output the differences to a new JSON file
    with open('diff_comparison/differences_output.json', 'w') as file:
        json.dump(output, file, indent=4, sort_keys=True)  # Format for readability

if __name__ == "__main__":
    main()
