import sys
import csv

# Take an two CSV file for queries with performance
# and calculate how much slower the first compared to the second



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


def compare_csv(file1, file2, output_file):
    # Read file1 into a dict {first_col: third_col}
    data1 = {}
    with open(file1, newline='') as f1:
        reader = csv.reader(f1)
        header1 = next(reader)  # skip header
        for row in reader:
            data1[row[0]] = float(row[2])  # first col → third col

    # Read file2 and compute differences
    results = []
    with open(file2, newline='') as f2:
        reader = csv.reader(f2)
        header2 = next(reader)  # skip header
        for row in reader:
            key = row[0]
            if key in data1:
                diff = (float(row[2]) - data1[key]) / float(row[2])
                percent = int(diff * 100 )
                results.append([key, percent])

    # Write results
    with open(output_file, "w", newline='') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(["id", "diff"])
        writer.writerows(results)

    return results


def main():

    if len(sys.argv) != 2:
        print("ERROR: expected <input CSV>")
        sys.exit(1)
        

    ex_csv = sys.argv[1]
    #nex_csv = sys.argv[2]
    #diff_csv = sys.argv[3]

    # Usage
    print(f"Total time with exchange in ms: {total_time_from_csv(ex_csv)}")
    #print(f"Total time without exchange in ms: {total_time_from_csv(nex_csv)}")

    #diff = compare_csv(ex_csv, nex_csv, diff_csv)

    #print("Percentage speed up of queries using non exchange as the denominator")
    #rint(diff)

if __name__ == "__main__":
    main()





