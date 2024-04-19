import json

def compare_json(data1, data2, path=""):
    differences = {}
    if isinstance(data1, dict) and isinstance(data2, dict):
        keys1, keys2 = set(data1.keys()), set(data2.keys())
        common_keys = keys1 & keys2
        added = keys2 - keys1
        removed = keys1 - keys2

        for key in common_keys:
            new_path = f"{path}/{key}" if path else key
            result = compare_json(data1[key], data2[key], new_path)
            if result:
                differences.update(result)
        
        for key in added:
            new_path = f"{path}/{key}" if path else key
            differences[new_path] = f"Added in updated JSON: {data2[key]}"
        
        for key in removed:
            new_path = f"{path}/{key}" if path else key
            differences[new_path] = f"Removed from original JSON: {data1[key]}"

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

    diff = compare_json(data1, data2)

    output = {}
    bucket_list = set(list(data1.keys()) + list(data2.keys()))  # Get all bucket names

    if diff:
        for path, change in diff.items():
            bucket_name = path.split('/')[0]
            if bucket_name not in output:
                output[bucket_name] = {}
            output[bucket_name][path] = change
        for bucket in bucket_list:
            if bucket not in output:
                output[bucket] = "OK"
    else:
        for bucket in bucket_list:
            output[bucket] = "OK"

    with open('differences_output_7.json', 'w') as file:
        json.dump(output, file, indent=4, sort_keys=True)  # sort_keys to maintain order

if __name__ == "__main__":
    main()
