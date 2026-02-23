import sys
import matplotlib.pyplot as plt

# Create a graph mapping execution time with number of workers from a CSV file
# CSV format expected: Num Workers, CudfExchange(sec), HttpExchange(sec)


def read_csv_data(filename):
    """Read CSV and return lists of workers and execution times."""
    workers = []
    cudf_times = []
    http_times = []
    http_workers = []

    with open(filename, newline='', encoding='utf-8') as csvfile:
        lines = csvfile.readlines()

        # Skip header
        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue

            # Split by whitespace (handles space-separated data)
            parts = line.split()
            if len(parts) < 2:
                continue

            try:
                num_workers = int(parts[0])
                workers.append(num_workers)
                cudf_times.append(float(parts[1]))
                if len(parts) >= 3:
                    http_times.append(float(parts[2]))
                    http_workers.append(num_workers)
            except ValueError as e:
                print(f"Skipping invalid row: {line} - {e}")

    return workers, cudf_times, http_times, http_workers


def create_plot(workers, cudf_times, http_times, http_workers, output_file, cudf_only=False):
    """Create and save the execution time vs workers plot."""
    plt.figure(figsize=(10, 6))

    # Plot CudfExchange times
    plt.plot(workers, cudf_times, marker='o', linewidth=2, label='CudfExchange')

    # Plot HttpExchange times if available and not cudf_only
    if http_times and not cudf_only:
        plt.plot(http_workers, http_times, marker='s', linewidth=2, label='HttpExchange')

    plt.legend()

    plt.xlabel('Number of Workers', fontsize=12)
    plt.ylabel('Execution Time (sec)', fontsize=12)
    plt.title('Elapsed Time By Number Workers for Q5 (SF=1000)', fontsize=14)
    plt.grid(True, alpha=0.3)
    plt.xticks(workers)
    plt.tight_layout()

    plt.savefig(output_file, dpi=150)
    print(f"Graph saved to: {output_file}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python compare_qtime_with_number_workers.py <input CSV> [output PNG] [--cudf-only]")
        print("Example: python compare_qtime_with_number_workers.py results/data.csv output.png")
        print("         python compare_qtime_with_number_workers.py results/data.csv output.png --cudf-only")
        sys.exit(1)

    input_csv = sys.argv[1]
    cudf_only = '--cudf-only' in sys.argv

    # Filter out flags from argv for positional args
    positional_args = [arg for arg in sys.argv[2:] if not arg.startswith('--')]
    output_file = positional_args[0] if positional_args else "execution_time_vs_workers.png"

    workers, cudf_times, http_times, http_workers = read_csv_data(input_csv)

    if not workers:
        print("ERROR: No valid data found in CSV")
        sys.exit(1)

    print(f"Loaded {len(workers)} data points")
    print(f"Workers: {workers}")
    print(f"CudfExchange times: {cudf_times}")
    if http_times:
        print(f"HttpExchange times: {http_times}")
        print(f"HttpExchange workers: {http_workers}")
    if cudf_only:
        print("Mode: CudfExchange only")

    create_plot(workers, cudf_times, http_times, http_workers, output_file, cudf_only)


if __name__ == "__main__":
    main()
