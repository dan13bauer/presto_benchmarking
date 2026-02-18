#!/usr/bin/env python3
"""Compare TPC-H SF100 query results between two execution directories.

Usage: compare_results.py <dir1> <dir2>

Handles:
  - Floating-point rounding differences (relative tolerance)
  - Non-deterministic row ordering (per-query sort keys)
  - WARNING/blank lines in output files (skipped)
"""

import csv
import glob
import io
import os
import sys


# ── Configuration ──────────────────────────────────────────────────────────
REL_TOL = 1e-6  # relative tolerance for float comparison


# ── Utility functions ──────────────────────────────────────────────────────

def parse_result_file(filepath):
    """Parse a result file into a list of rows (list of strings).
    Skips blank lines and WARNING lines."""
    rows = []
    with open(filepath, "r") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("WARNING:"):
                continue
            reader = csv.reader(io.StringIO(stripped))
            for row in reader:
                rows.append(row)
    return rows


def try_float(s):
    """Try to parse string as float. Returns (is_float, value)."""
    try:
        return True, float(s)
    except (ValueError, TypeError):
        return False, s


def values_match(a, b, rel_tol=REL_TOL):
    """Compare two string values; use relative tolerance for floats."""
    is_fa, fa = try_float(a)
    is_fb, fb = try_float(b)
    if is_fa and is_fb:
        if fa == fb:
            return True
        denom = max(abs(fa), abs(fb))
        if denom == 0:
            return True
        return abs(fa - fb) / denom < rel_tol
    return a == b


def rows_match(row1, row2, rel_tol=REL_TOL):
    """Compare two rows field-by-field."""
    if len(row1) != len(row2):
        return False
    return all(values_match(a, b, rel_tol) for a, b in zip(row1, row2))


def sort_rows(rows, key_cols):
    """Sort rows by the given column indices for deterministic ordering.
    Numeric columns sort numerically; string columns sort lexicographically."""
    def sort_key(row):
        parts = []
        for c in key_cols:
            is_f, fv = try_float(row[c])
            if is_f:
                parts.append((0, fv, ""))
            else:
                parts.append((1, 0.0, row[c]))
        return parts
    return sorted(rows, key=sort_key)


def compare_query(query_num, cpu_rows, gpu_rows, sort_cols=None, rel_tol=REL_TOL):
    """Generic comparison engine.

    Args:
        query_num: query number (for messages)
        cpu_rows:  parsed CPU result rows
        gpu_rows:  parsed GPU result rows
        sort_cols: column indices to sort by (None = already deterministically ordered)
        rel_tol:   relative tolerance for floats

    Returns:
        (passed: bool, messages: list[str])
    """
    msgs = []

    # 1) Row count check
    if len(cpu_rows) != len(gpu_rows):
        msgs.append(f"  Row count mismatch: CPU={len(cpu_rows)}, GPU={len(gpu_rows)}")
        return False, msgs

    if not cpu_rows:
        msgs.append("  Both results are empty")
        return True, msgs

    # 2) Column count check
    if len(cpu_rows[0]) != len(gpu_rows[0]):
        msgs.append(
            f"  Column count mismatch on row 1: CPU={len(cpu_rows[0])}, GPU={len(gpu_rows[0])}"
        )
        return False, msgs

    # 3) Sort if needed
    if sort_cols is not None:
        cpu_rows = sort_rows(cpu_rows, sort_cols)
        gpu_rows = sort_rows(gpu_rows, sort_cols)

    # 4) Row-by-row comparison
    mismatches = 0
    max_show = 5
    for i, (cr, gr) in enumerate(zip(cpu_rows, gpu_rows)):
        if not rows_match(cr, gr, rel_tol):
            mismatches += 1
            if mismatches <= max_show:
                msgs.append(f"  Row {i+1} mismatch:")
                msgs.append(f"    CPU: {cr}")
                msgs.append(f"    GPU: {gr}")
                diffs = []
                for j, (a, b) in enumerate(zip(cr, gr)):
                    if not values_match(a, b, rel_tol):
                        diffs.append(f"col {j}: '{a}' vs '{b}'")
                if len(cr) != len(gr):
                    diffs.append(f"column count: {len(cr)} vs {len(gr)}")
                msgs.append(f"    Diffs: {', '.join(diffs)}")

    if mismatches > max_show:
        msgs.append(f"  ... and {mismatches - max_show} more mismatched rows")

    if mismatches > 0:
        msgs.append(f"  Total mismatches: {mismatches} / {len(cpu_rows)} rows")
        return False, msgs

    msgs.append(f"  All {len(cpu_rows)} rows match")
    return True, msgs


# ── Per-query comparison functions ─────────────────────────────────────────
# Each function documents the TPC-H query semantics and ORDER BY clause,
# then delegates to compare_query with appropriate sort_cols.


def compare_q01(cpu, gpu):
    """Q1: Pricing Summary Report
    ORDER BY l_returnflag, l_linestatus  (deterministic string keys)
    Cols: returnflag, linestatus, sum_qty, sum_base_price, sum_disc_price,
          sum_charge, avg_qty, avg_price, avg_disc, count_order"""
    return compare_query(1, cpu, gpu, sort_cols=None)


def compare_q02(cpu, gpu):
    """Q2: Minimum Cost Supplier (top 100)
    ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
    Acctbal may have rounding diffs causing order changes.
    Sort by s_name(1) + p_partkey(3) for deterministic comparison."""
    return compare_query(2, cpu, gpu, sort_cols=[1, 3])


def compare_q03(cpu, gpu):
    """Q3: Shipping Priority (top 10)
    ORDER BY revenue DESC, o_orderdate
    Revenue may have rounding diffs causing order changes.
    Sort by l_orderkey(0) for deterministic comparison."""
    return compare_query(3, cpu, gpu, sort_cols=[0])


def compare_q04(cpu, gpu):
    """Q4: Order Priority Checking
    ORDER BY o_orderpriority  (deterministic string key)
    Cols: orderpriority, order_count"""
    return compare_query(4, cpu, gpu, sort_cols=None)


def compare_q05(cpu, gpu):
    """Q5: Local Supplier Volume
    ORDER BY revenue DESC  (single row in this result set)
    Cols: n_name, revenue"""
    return compare_query(5, cpu, gpu, sort_cols=None)


def compare_q06(cpu, gpu):
    """Q6: Forecasting Revenue Change  (single aggregate)
    Cols: revenue"""
    return compare_query(6, cpu, gpu, sort_cols=None)


def compare_q07(cpu, gpu):
    """Q7: Volume Shipping
    ORDER BY supp_nation, cust_nation, l_year  (deterministic keys)
    Cols: supp_nation, cust_nation, l_year, revenue"""
    return compare_query(7, cpu, gpu, sort_cols=None)


def compare_q08(cpu, gpu):
    """Q8: National Market Share
    ORDER BY o_year  (deterministic integer key)
    Cols: o_year, mkt_share"""
    return compare_query(8, cpu, gpu, sort_cols=None)


def compare_q09(cpu, gpu):
    """Q9: Product Type Profit Measure
    ORDER BY nation, o_year DESC  (deterministic keys)
    Cols: nation, o_year, sum_profit"""
    return compare_query(9, cpu, gpu, sort_cols=None)


def compare_q10(cpu, gpu):
    """Q10: Returned Item Reporting (top 20)
    ORDER BY revenue DESC
    Revenue may have rounding diffs. Sort by c_custkey(0) for stability."""
    return compare_query(10, cpu, gpu, sort_cols=[0])


def compare_q11(cpu, gpu):
    """Q11: Important Stock Identification (92698 rows)
    ORDER BY value DESC
    Value may have rounding diffs. Sort by ps_partkey(0) for stability."""
    return compare_query(11, cpu, gpu, sort_cols=[0])


def compare_q12(cpu, gpu):
    """Q12: Shipping Modes and Order Priority
    ORDER BY l_shipmode  (deterministic string key)
    Cols: l_shipmode, high_line_count, low_line_count"""
    return compare_query(12, cpu, gpu, sort_cols=None)


def compare_q13(cpu, gpu):
    """Q13: Customer Distribution
    ORDER BY custdist DESC, c_count DESC  (integer keys, deterministic)
    Cols: c_count, custdist"""
    return compare_query(13, cpu, gpu, sort_cols=None)


def compare_q14(cpu, gpu):
    """Q14: Promotion Effect  (single aggregate)
    Cols: promo_revenue"""
    return compare_query(14, cpu, gpu, sort_cols=None)


def compare_q15(cpu, gpu):
    """Q15: Top Supplier Query (top 20)
    Sort by first column (s_suppkey or c_custkey) for stability."""
    return compare_query(15, cpu, gpu, sort_cols=[0])


def compare_q16(cpu, gpu):
    """Q16: Parts/Supplier Relationship (27842 rows)
    ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
    Many ties in supplier_cnt. Sort by p_brand(0), p_type(1), p_size(2)
    for deterministic comparison."""
    return compare_query(16, cpu, gpu, sort_cols=[0, 1, 2])


def compare_q17(cpu, gpu):
    """Q17: Small-Quantity-Order Revenue  (single aggregate)
    Cols: avg_yearly"""
    return compare_query(17, cpu, gpu, sort_cols=None)


def compare_q18(cpu, gpu):
    """Q18: Large Volume Customer (top 100)
    ORDER BY o_totalprice DESC, o_orderdate
    Totalprice may have rounding diffs.
    Sort by c_name(0) + o_orderkey(2) for deterministic comparison."""
    return compare_query(18, cpu, gpu, sort_cols=[0, 2])


def compare_q19(cpu, gpu):
    """Q19: Discounted Revenue  (single aggregate)
    Cols: revenue"""
    return compare_query(19, cpu, gpu, sort_cols=None)


def compare_q20(cpu, gpu):
    """Q20: Potential Part Promotion (17971 rows)
    ORDER BY s_name  (deterministic string key)
    Cols: s_name, s_address"""
    return compare_query(20, cpu, gpu, sort_cols=None)


def compare_q21(cpu, gpu):
    """Q21: Suppliers Who Kept Orders Waiting (top 100)
    ORDER BY numwait DESC, s_name
    Numwait has ties but s_name breaks them.
    Sort by s_name(0) for deterministic comparison."""
    return compare_query(21, cpu, gpu, sort_cols=[0])


def compare_q22(cpu, gpu):
    """Q22: Global Sales Opportunity
    ORDER BY cntrycode  (deterministic string key)
    Cols: cntrycode, numcust, totacctbal"""
    return compare_query(22, cpu, gpu, sort_cols=None)


# ── Main ───────────────────────────────────────────────────────────────────

COMPARATORS = {
    1: compare_q01,   2: compare_q02,   3: compare_q03,   4: compare_q04,
    5: compare_q05,   6: compare_q06,   7: compare_q07,   8: compare_q08,
    9: compare_q09,  10: compare_q10,  11: compare_q11,  12: compare_q12,
   13: compare_q13,  14: compare_q14,  15: compare_q15,  16: compare_q16,
   17: compare_q17,  18: compare_q18,  19: compare_q19,  20: compare_q20,
   21: compare_q21,  22: compare_q22,
}


def count_result_files(directory):
    """Count query_*.res files in a directory."""
    return len(glob.glob(os.path.join(directory, "query_*.res")))


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <dir1> <dir2>", file=sys.stderr)
        return 2

    dir1 = sys.argv[1]
    dir2 = sys.argv[2]

    # Validate directories
    for d in (dir1, dir2):
        if not os.path.isdir(d):
            print(f"Error: '{d}' is not a directory", file=sys.stderr)
            return 2

    # Check that both directories contain the same number of result files
    count1 = count_result_files(dir1)
    count2 = count_result_files(dir2)
    if count1 != count2:
        print(
            f"Error: result file count mismatch: "
            f"'{dir1}' has {count1}, '{dir2}' has {count2}",
            file=sys.stderr,
        )
        return 2
    if count1 == 0:
        print(f"Error: no query_*.res files found in either directory", file=sys.stderr)
        return 2

    print(f"Comparing: {dir1}  vs  {dir2}")
    print(f"Result files per directory: {count1}\n")

    passed = 0
    failed = 0

    for q in range(1, 23):
        filename = f"query_{q:02d}.res"
        path1 = os.path.join(dir1, filename)
        path2 = os.path.join(dir2, filename)

        if not os.path.exists(path1) or not os.path.exists(path2):
            print(f"Query {q:2d}: SKIP (file not found)")
            continue

        rows1 = parse_result_file(path1)
        rows2 = parse_result_file(path2)

        comparator = COMPARATORS[q]
        doc_line = comparator.__doc__.strip().splitlines()[0]
        ok, msgs = comparator(rows1, rows2)

        status = "PASS" if ok else "FAIL"
        print(f"Query {q:2d}: {status}  [{doc_line}]")
        for m in msgs:
            print(m)

        if ok:
            passed += 1
        else:
            failed += 1

    print(f"\n{'='*70}")
    print(f"Summary: {passed} passed, {failed} failed, {passed + failed} total")
    print(f"Float tolerance: relative {REL_TOL}")
    if failed == 0:
        print("All queries match!")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
