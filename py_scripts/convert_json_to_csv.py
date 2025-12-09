#!/usr/bin/env python3
"""
Script to convert benchmark JSON results to CSV format.
Reads timing data in milliseconds and outputs to CSV with times in seconds.
"""

import json
import csv
import argparse
from pathlib import Path


def convert_benchmark_to_csv(json_path, csv_path=None):
    """
    Read benchmark JSON file and convert to CSV.

    Args:
        json_path: Path to the benchmark JSON file
        csv_path: Path to output CSV file (defaults to same directory with .csv extension)
    """
    # Default CSV path if not specified
    if csv_path is None:
        json_path_obj = Path(json_path)
        csv_path = json_path_obj.parent / f"{json_path_obj.stem}.csv"

    # Read JSON file
    with open(json_path, 'r') as f:
        data = json.load(f)

    # Extract benchmark data (assuming tpch is the benchmark name)
    benchmark_data = data.get('tpch', {})
    agg_times = benchmark_data.get('agg_times_ms', {})

    avg_times = agg_times.get('avg', {})
    min_times = agg_times.get('min', {})
    max_times = agg_times.get('max', {})

    # Get all query names
    query_names = sorted(set(avg_times.keys()) | set(min_times.keys()) | set(max_times.keys()))

    # Write CSV file
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Query Name', 'Avg Time (seconds)', 'Min Time (seconds)', 'Max Time (seconds)'])

        for query in query_names:
            avg_sec = avg_times.get(query, 0) / 1000
            min_sec = min_times.get(query, 0) / 1000
            max_sec = max_times.get(query, 0) / 1000

            writer.writerow([query, f'{avg_sec:.3f}', f'{min_sec:.3f}', f'{max_sec:.3f}'])

    print(f"CSV file created: {csv_path}")
    print(f"Total queries: {len(query_names)}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Convert benchmark JSON results to CSV format.'
    )
    parser.add_argument(
        'input',
        help='Path to the input JSON file'
    )
    parser.add_argument(
        '-o', '--output',
        help='Path to the output CSV file (optional, defaults to input filename with .csv extension)',
        default=None
    )

    args = parser.parse_args()
    convert_benchmark_to_csv(args.input, args.output)
