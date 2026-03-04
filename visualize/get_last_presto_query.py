#!/usr/bin/env python3
"""
Script: get_last_presto_query.py
Purpose: Connect to Presto and retrieve JSON details about the last N executed queries

Usage:
    python3 get_last_presto_query.py [presto_url] [output_file]

Examples:
    # Get last query from default localhost Presto, print to stdout
    python3 get_last_presto_query.py

    # Get from specific Presto server, save to file
    python3 get_last_presto_query.py http://presto.example.com:8080 last_query.json

    # Get detailed query info
    python3 get_last_presto_query.py http://localhost:8080 last_query_detailed.json --detailed

    # Get last 10 queries into numbered files (query_metrics_00.json .. query_metrics_09.json)
    python3 get_last_presto_query.py http://localhost:8080 -n 10 --output-prefix query_metrics
"""

import requests
import json
import sys
import argparse
import traceback
from datetime import datetime
from typing import Optional, Dict, Any, List


def fetch_json(url: str) -> Any:
    """
    Fetch JSON data from the given URL.
    Raises on bad status codes or invalid JSON.
    """
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError as e:
        raise Exception(f"Connection error: Cannot reach {url}. Is Presto running?") from e
    except requests.exceptions.Timeout:
        raise Exception(f"Timeout: Request to {url} took too long") from None
    except requests.exceptions.HTTPError as e:
        raise Exception(f"HTTP error: {e.response.status_code} - {e.response.reason}") from e
    except json.JSONDecodeError as e:
        raise Exception(f"Invalid JSON response: {e}") from e


def get_last_query_id(presto_url: str) -> Optional[str]:
    """
    Fetch list of all queries and return the ID of the most recent one.
    """
    query_list_url = f"{presto_url}/v1/query"
    print(f"[*] Fetching query list from: {query_list_url}", file=sys.stderr)

    try:
        queries = fetch_json(query_list_url)

        if not isinstance(queries, list):
            raise ValueError("Expected JSON array of queries")

        if not queries:
            raise ValueError("No queries found on Presto server")

        # Get the last query (most recent) - queries are typically ordered by creation time
        # Presto returns queries in reverse chronological order (newest first)
        last_query_id = queries[0].get("queryId")

        if not last_query_id:
            raise ValueError("Query ID not found in response")

        print(f"[✓] Found {len(queries)} queries", file=sys.stderr)
        print(f"[✓] Last query ID: {last_query_id}", file=sys.stderr)

        return last_query_id

    except Exception as e:
        print(f"[✗] Error fetching query list: {e}", file=sys.stderr)
        raise


def get_query_details(presto_url: str, query_id: str) -> Dict[str, Any]:
    """
    Fetch detailed information about a specific query.
    """
    query_detail_url = f"{presto_url}/v1/query/{query_id}"
    print(f"[*] Fetching query details from: {query_detail_url}", file=sys.stderr)

    try:
        query_details = fetch_json(query_detail_url)
        print(f"[✓] Retrieved detailed query info", file=sys.stderr)
        return query_details

    except Exception as e:
        print(f"[✗] Error fetching query details: {e}", file=sys.stderr)
        raise


def get_last_query_basic(presto_url: str) -> Dict[str, Any]:
    """
    Get basic info about the last query from the query list.
    Faster than fetching full details.
    """
    query_list_url = f"{presto_url}/v1/query"
    print(f"[*] Fetching query list from: {query_list_url}", file=sys.stderr)

    try:
        queries = fetch_json(query_list_url)

        if not isinstance(queries, list):
            raise ValueError("Expected JSON array of queries")

        if not queries:
            raise ValueError("No queries found on Presto server")

        last_query = queries[0]  # First item is most recent
        print(f"[✓] Retrieved basic query info for {last_query.get('queryId')}", file=sys.stderr)
        return last_query

    except Exception as e:
        print(f"[✗] Error fetching query: {e}", file=sys.stderr)
        raise


def get_last_n_queries(presto_url: str, n: int, detailed: bool = False) -> List[Dict[str, Any]]:
    """
    Fetch the last N queries from Presto.
    Returns a list ordered oldest-first (index 0 = oldest).
    If detailed=True, fetches full details for each query via /v1/query/{id}.
    """
    query_list_url = f"{presto_url}/v1/query"
    print(f"[*] Fetching query list from: {query_list_url}", file=sys.stderr)

    queries = fetch_json(query_list_url)

    if not isinstance(queries, list):
        raise ValueError("Expected JSON array of queries")

    if not queries:
        raise ValueError("No queries found on Presto server")

    # Take the first N (newest-first from API), clamp to available count
    count = min(n, len(queries))
    if count < n:
        print(f"[!] Requested {n} queries but only {count} available", file=sys.stderr)

    selected = queries[:count]
    print(f"[✓] Selected {count} queries (newest first)", file=sys.stderr)

    if detailed:
        results = []
        for i, q in enumerate(selected):
            qid = q.get("queryId")
            print(f"[*] Fetching details for query {i+1}/{count}: {qid}", file=sys.stderr)
            results.append(get_query_details(presto_url, qid))
        selected = results

    # Reverse so index 0 = oldest query
    selected.reverse()
    return selected


def format_json_output(data: Dict[str, Any], pretty: bool = True) -> str:
    """
    Format JSON data for output.
    """
    if pretty:
        return json.dumps(data, indent=2, default=str)
    else:
        return json.dumps(data, default=str)


def save_to_file(data: str, output_path: str) -> None:
    """
    Save JSON data to file.
    """
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(data)
        print(f"[✓] Saved to {output_path}", file=sys.stderr)
    except IOError as e:
        raise Exception(f"Cannot write to {output_path}: {e}") from e


def print_summary(query_data: Dict[str, Any]) -> None:
    """
    Print a human-readable summary of the query.
    """
    print("\n" + "="*80, file=sys.stderr)
    print("QUERY SUMMARY", file=sys.stderr)
    print("="*80, file=sys.stderr)

    query_id = query_data.get("queryId", "N/A")
    state = query_data.get("state", "N/A")
    user = query_data.get("session", {}).get("user", "N/A")
    query_string = query_data.get("query", "")[:100]

    stats = query_data.get("queryStats", {})
    create_time = stats.get("createTime", "N/A")
    elapsed = stats.get("elapsedTime", "N/A")
    queue_time = stats.get("queuedTime", "N/A")

    print(f"Query ID:      {query_id}", file=sys.stderr)
    print(f"State:         {state}", file=sys.stderr)
    print(f"User:          {user}", file=sys.stderr)
    print(f"Created:       {create_time}", file=sys.stderr)
    print(f"Elapsed Time:  {elapsed}", file=sys.stderr)
    print(f"Queued Time:   {queue_time}", file=sys.stderr)
    print(f"Query (first 100 chars): {query_string}...", file=sys.stderr)
    print("="*80 + "\n", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Retrieve JSON details about the last N queries executed on Presto",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Get last query from default localhost, print to stdout:
    python3 get_last_presto_query.py

  Get from specific server, save to file:
    python3 get_last_presto_query.py http://presto.example.com:8080 last_query.json

  Get full detailed info:
    python3 get_last_presto_query.py http://localhost:8080 query_details.json --detailed

  Get last 10 queries into numbered files:
    python3 get_last_presto_query.py http://localhost:8080 -n 10 --output-prefix query_metrics

  Get last 5 detailed queries:
    python3 get_last_presto_query.py http://localhost:8080 -n 5 --output-prefix query_metrics --detailed

  Get summary without JSON:
    python3 get_last_presto_query.py --summary-only
        """
    )

    parser.add_argument(
        "url",
        nargs="?",
        default="http://localhost:8080",
        help="Presto server URL (default: http://localhost:8080)"
    )

    parser.add_argument(
        "output",
        nargs="?",
        default=None,
        help="Output file path (default: print to stdout)"
    )

    parser.add_argument(
        "--detailed",
        action="store_true",
        help="Fetch full detailed query info (slower but more complete)"
    )

    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Print summary only, don't output JSON"
    )

    parser.add_argument(
        "--compact",
        action="store_true",
        help="Output compact JSON (no pretty-printing)"
    )

    parser.add_argument(
        "-n", "--num-queries",
        type=int,
        default=1,
        help="Number of recent queries to fetch (default: 1)"
    )

    parser.add_argument(
        "--output-prefix",
        default="query_metrics",
        help="Filename prefix when fetching multiple queries (default: query_metrics). "
             "Files will be named <prefix>_00.json, <prefix>_01.json, ... "
             "where 00 is the oldest query."
    )

    args = parser.parse_args()

    # Normalize URL
    url = args.url
    if not url.startswith("http://") and not url.startswith("https://"):
        url = f"http://{url}"

    try:
        print(f"[*] Connecting to Presto at {url}", file=sys.stderr)
        n = args.num_queries

        if n > 1:
            # Multi-query mode: fetch last N queries into numbered files
            queries = get_last_n_queries(url, n, detailed=args.detailed)
            width = len(str(len(queries) - 1))  # digit width for zero-padding
            width = max(width, 2)  # minimum 2 digits

            for i, query_data in enumerate(queries):
                print_summary(query_data)

                if not args.summary_only:
                    json_output = format_json_output(query_data, pretty=not args.compact)
                    filename = f"{args.output_prefix}_{i+1:0{width}d}.json"
                    save_to_file(json_output, filename)

            print(f"[✓] Saved {len(queries)} queries to {args.output_prefix}_00.json .. "
                  f"{args.output_prefix}_{len(queries)-1:0{width}d}.json", file=sys.stderr)
        else:
            # Single-query mode (original behavior)
            if args.detailed:
                query_id = get_last_query_id(url)
                query_data = get_query_details(url, query_id)
            else:
                query_data = get_last_query_basic(url)

            print_summary(query_data)

            if not args.summary_only:
                json_output = format_json_output(query_data, pretty=not args.compact)

                if args.output:
                    save_to_file(json_output, args.output)
                else:
                    print(json_output)

        print("[✓] Done", file=sys.stderr)
        return 0

    except Exception as e:
        print(f"[✗] Error: {e}", file=sys.stderr)
        if "--verbose" in sys.argv:
            traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
