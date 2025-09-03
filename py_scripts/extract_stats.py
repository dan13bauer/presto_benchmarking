import requests
import csv
import sys
import re

# List your fields here. Use dot notation for nested lookups.
COOKED_NAMES= [
    "queryName",
    "scaleFactor",
    "timeMillsecs"
]

FIELD_NAMES = [
    "queryStats.createTime",
    "queryId",
    "queryStats.elapsedTime",
    "state"
]


def time_to_ms(time_str):
    # Remove spaces and handle comma as decimal separator
    time_str = time_str.replace(" ", "").replace(",", ".").lower()
    # Extract the numeric value and unit using regex
    match = re.match(r'([0-9.]+)([a-z]+)', time_str)
    if not match:
        raise ValueError(f"Invalid time string: {time_str}")
    value, unit = match.groups()
    value = float(value)
    # Convert based on unit
    if unit in ['ms', 'millisecond', 'milliseconds']:
        return value
    elif unit in ['s', 'sec', 'second', 'seconds']:
        return value * 1000
    elif unit in ['m', 'min', 'minute', 'minutes']:
        return value * 60 * 1000
    elif unit in ['h', 'hr', 'hour', 'hours']:
        return value * 60 * 60 * 1000
    else:
        raise ValueError(f"Unknown unit: {unit}")


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

def get_query(elem):
    """
    Return the TPC-H query from the comment in the query text, None of doesn't exist
    """
    query_string = get_value(elem,"query")
    match = re.search(r'TPCH\s+(\w+)', query_string)
    if match:
        return match.group(1)  # the word after TPCH
    return None



def get_scale_factor(elem):
    """
    Return the scale factor used for the query, this is extracted from  the schema
    """
    schema_string = get_value(elem, "session.schema")
    match = re.search(r'(\w+)_parquet', schema_string)
    if match:
        return match.group(1)  # the word after TPCH
    return None

def get_elapsed_time(elem):
    query_time = get_value(elem,"queryStats.elapsedTime")
    time_in_ms = time_to_ms(query_time)
    return str(int(time_in_ms))

def get_cooked_row(elem):
    query_name = get_query(elem)
    scale_factor = get_scale_factor(elem)
    query_time = get_elapsed_time(elem)
    return(query_name, scale_factor,query_time)

def extract_records(json_array, field_names):
    """
    For each element in the JSON array, pull out all fields
    listed in `field_names` (including nested ones).
    Returns a list of tuples matching the order in `field_names`.
    """
    records = []
    for elem in json_array:
        row = tuple(get_value(elem, name) for name in field_names)
        query_row = get_cooked_row(elem) + row
        records.append(query_row)

    records.sort()    
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
        write_csv(records, COOKED_NAMES + FIELD_NAMES, output_csv)
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
