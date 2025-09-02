import requests
import csv
import sys

# List your fields here. Use dot notation for nested lookups.
FIELD_NAMES = [
    "queryStats.createTime",
    "queryId",
    "queryStats.elapsedTime",
    "state"
]

def fetch_json(url):
    """
    Fetch JSON data from the given URL.
    Raises on bad status codes or invalid JSON.
    """
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def get_value(obj, field_name):
    """
    Traverse `obj` following the dot-separated keys in `field_name`.
    Returns "" if any key is missing or if a non-dict is encountered.
    """
    parts = field_name.split(".")
    val = obj
    for key in parts:
        if isinstance(val, dict):
            val = val.get(key, "")
        else:
            return ""
    return val

def extract_records(json_array, field_names):
    """
    For each element in the JSON array, pull out all fields
    listed in `field_names` (including nested ones).
    Returns a list of tuples matching the order in `field_names`.
    """
    records = []
    for elem in json_array:
        row = tuple(get_value(elem, name) for name in field_names)
        records.append(row)
    return records

def write_csv(records, field_names, output_path):
    """
    Write CSV with header row = `field_names` and subsequent rows = `records`.
    """
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(field_names)
        writer.writerows(records)

def main():
    # Default URL and output file – override via CLI if desired
    url = "http://sally:19300/v1/query/"
    output_csv = "query_stats.csv"

    if len(sys.argv) >= 2:
        url = sys.argv[1]
    if len(sys.argv) >= 3:
        output_csv = sys.argv[2]

    try:
        data = fetch_json(url)
        if not isinstance(data, list):
            print("ERROR: expected JSON array at root.")
            sys.exit(1)

        records = extract_records(data, FIELD_NAMES)
        write_csv(records, FIELD_NAMES, output_csv)
        print(f"✅ Wrote {len(records)} records to {output_csv}")

    except requests.RequestException as e:
        print(f"Network error: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"Invalid JSON: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
