#!/bin/bash

# Expected number of lines
EXPECTED_LINES=175

# Expected last lines (reference output)
EXPECTED_LAST_LINES=(
    '"VIETNAM","1998","2.7362203527692696E10"'
    '"VIETNAM","1997","4.667985730656909E10"'
    '"VIETNAM","1996","4.68235439956468E10"'
    '"VIETNAM","1995","4.670427873184009E10"'
    '"VIETNAM","1994","4.6619569594595505E10"'
    '"VIETNAM","1993","4.6553281815975494E10"'
    '"VIETNAM","1992","4.682291315130171E10"'
)

# Number of last lines to check
NUM_LAST_LINES=${#EXPECTED_LAST_LINES[@]}

# Function to extract and compare numbers with 10-digit precision
compare_lines() {
    local line1="$1"
    local line2="$2"

    # Extract the numeric value (third field)
    local num1=$(echo "$line1" | awk -F',' '{print $3}' | tr -d '"')
    local num2=$(echo "$line2" | awk -F',' '{print $3}' | tr -d '"')

    # Extract country and year for comparison
    local country_year1=$(echo "$line1" | awk -F',' '{print $1 "," $2}')
    local country_year2=$(echo "$line2" | awk -F',' '{print $1 "," $2}')

    # Check if country and year match
    if [ "$country_year1" != "$country_year2" ]; then
        return 1
    fi

    # Compare numbers with 10 significant digits
    # Convert to comparable format by taking first 10 significant digits
    local normalized1=$(echo "$num1" | awk '{printf "%.10g", $1}')
    local normalized2=$(echo "$num2" | awk '{printf "%.10g", $1}')

    # Check if the normalized values are close enough
    local diff=$(echo "$normalized1 $normalized2" | awk '{
        if ($1 == 0 && $2 == 0) { print 0; exit }
        if ($1 == 0 || $2 == 0) { print 1; exit }
        ratio = $1 / $2
        if (ratio > 0.9999999999 && ratio < 1.0000000001) print 0
        else print 1
    }')

    return $diff
}

iteration=0

echo "Starting query stability check..."
echo "Expected: $EXPECTED_LINES lines"
echo "Checking last $NUM_LAST_LINES lines for consistency"
echo "================================================"

while true; do
    iteration=$((iteration + 1))
    echo -n "Iteration $iteration: "

    # Run the query and capture output
    output=$(./run_nvidia_query.sh 9 2>&1)
    exit_code=$?

    # Check if command succeeded
    if [ $exit_code -ne 0 ]; then
        echo "FAILED - Command exited with code $exit_code"
        echo "Output:"
        echo "$output"
        exit 1
    fi

    # Count lines
    line_count=$(echo "$output" | wc -l)

    echo "$output" | tail -n 7

    # Check line count
    if [ "$line_count" -ne "$EXPECTED_LINES" ]; then
        echo "FAILED - Expected $EXPECTED_LINES lines, got $line_count lines"
        echo "Output:"
        echo "$output"
        exit 1
    fi

    # Get last N lines from output
    actual_last_lines=$(echo "$output" | tail -n "$NUM_LAST_LINES")
    
    # Compare each of the last lines
    line_num=0
    mismatch=0
    while IFS= read -r actual_line; do
        expected_line="${EXPECTED_LAST_LINES[$line_num]}"

        if ! compare_lines "$actual_line" "$expected_line"; then
            echo "FAILED - Mismatch in line $((line_count - NUM_LAST_LINES + line_num + 1))"
            echo "Expected: $expected_line"
            echo "Actual:   $actual_line"
            mismatch=1
            break
        fi

        line_num=$((line_num + 1))
    done <<< "$actual_last_lines"

    if [ $mismatch -eq 1 ]; then
        echo ""
        echo "Full output:"
        echo "$output"
        #exit 1
    fi

    echo "OK ($line_count lines, last $NUM_LAST_LINES lines match)"

    # Optional: add a small delay to avoid overwhelming the system
    # sleep 0.1
done
