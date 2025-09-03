import sys
import csv

def total_time_from_csv(filename):
    total = 0.0
    with open(filename, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) < 3:
                continue
            try:
                ms = float(row[2])
                total += ms
            except ValueError:
                print(f"Skipping invalid value: {row[2]}")
    return total


def main():

    if len(sys.argv) != 2:
        print("ERROR: expected CSV.")
        sys.exit(1)
        

    input_csv = sys.argv[1]

    # Usage
    print(f"Total time in ms: {total_time_from_csv(input_csv)}")


if __name__ == "__main__":
    main()

