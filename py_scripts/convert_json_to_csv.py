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
    median_times = agg_times.get('median', {})
    geometric_mean_times = agg_times.get('geometric_mean', {})
    failed_queries = benchmark_data.get('failed_queries', {})

    # Get all query names (including successful and failed queries)
    query_names = set(avg_times.keys()) | set(min_times.keys()) | set(max_times.keys()) | set(median_times.keys()) | set(geometric_mean_times.keys()) | set(failed_queries.keys())

    # Sort by numeric value after 'Q' instead of alphabetically
    query_names = sorted(query_names, key=lambda x: int(x[1:]))

    # Write CSV file
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Query Name', 'Avg Time (seconds)', 'Min Time (seconds)', 'Max Time (seconds)', 'Median Time (seconds)', 'Geometric Mean Time (seconds)', 'Status'])

        for query in query_names:
            if query in failed_queries:
                # Query failed - show error message
                error_msg = failed_queries[query]
                writer.writerow([query, 'FAILED', 'FAILED', 'FAILED', 'FAILED', 'FAILED', error_msg])
            else:
                # Query succeeded - show times
                avg_sec = avg_times.get(query, 0) / 1000
                min_sec = min_times.get(query, 0) / 1000
                max_sec = max_times.get(query, 0) / 1000
                median_sec = median_times.get(query, 0) / 1000
                geometric_mean_sec = geometric_mean_times.get(query, 0) / 1000
                writer.writerow([query, f'{avg_sec:.3f}', f'{min_sec:.3f}', f'{max_sec:.3f}', f'{median_sec:.3f}', f'{geometric_mean_sec:.3f}', 'SUCCESS'])

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
