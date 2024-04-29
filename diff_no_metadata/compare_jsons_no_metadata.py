import json

def compare_json(data1, data2, path=""):
    differences = {}

    def clean_response_metadata(data):
        if isinstance(data, dict):
            if 'ResponseMetadata' in data:
                data.pop('ResponseMetadata')
            for key in list(data.keys()):
                data[key] = clean_response_metadata(data[key])
        elif isinstance(data, list):
            return [clean_response_metadata(item) for item in data]
        return data

    data1 = clean_response_metadata(data1)
    data2 = clean_response_metadata(data2)

    if isinstance(data1, dict) and isinstance(data2, dict):
        keys1, keys2 = set(data1.keys()), set(data2.keys())
        common_keys = keys1 & keys2
        added = keys2 - keys1
        removed = keys1 - keys2

        for key in added:
            new_path = f"{path}/{key}" if path else key
            differences[new_path] = f"Added in updated JSON: {data2[key]}"
        for key in removed:
            new_path = f"{path}/{key}" if path else key
            differences[new_path] = f"Removed from original JSON: {data1[key]}"

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

    output = {"Buckets": {}}

    # Compare each key (bucket) in the combined set of keys from both files
    for key in set(data1.keys()).union(data2.keys()):
        bucket_details = {}
        bucket_diff = compare_json(data1.get(key, {}), data2.get(key, {}), key)
        bucket_details["Status"] = "OK" if not bucket_diff else "Differences"
        bucket_details["Operations"] = bucket_diff if bucket_diff else {}
        output["Buckets"][key] = bucket_details

    with open('diff_comparison/differences_output_no_metadata.json', 'w') as file:
        json.dump(output, file, indent=4, sort_keys=True)

if __name__ == "__main__":
    main()
