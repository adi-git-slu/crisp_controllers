#!/usr/bin/env python3
# Copyright (c) 2024 Analog Devices Inc.
# Licensed under the Apache License, Version 2.0

import argparse
import csv
import json
import os
import re

LOG_RE = re.compile(r"^\[\w+\]\s+\[(?P<ts>\d+\.\d+)\]\s+\[[^\]]+\]:\s+(?P<body>.*)$")

KEYVAL_RE = re.compile(r"^(?P<key>[^:]+):\s*(?P<values>.*)$")
SPLIT_RE = re.compile(r"[,\s]+")


def sanitize(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"\s+", "_", s)
    return re.sub(r"[^a-z0-9_]", "", s)


def parse_values(s: str):
    return [float(v) for v in SPLIT_RE.split(s.strip()) if v]


def load_config(config_path: str) -> dict:
    """Load and parse configuration file."""
    with open(config_path, encoding="utf-8") as f:
        return json.load(f)


def prepare_columns(keys: dict) -> tuple[list, dict]:
    """Prepare column headers and key-to-column mappings."""
    columns = []
    key_cols = {}
    for key, fields in keys.items():
        key_cols[key] = [f"{sanitize(key)}.{sanitize(f)}" for f in fields]
        columns.extend(key_cols[key])
    return columns, key_cols


def parse_log_file(
    input_file: str, keys: dict, key_cols: dict, columns: list
) -> tuple[list, set]:
    """Parse log file and extract data rows."""
    rows = []
    current_row = None
    found_keys = set()

    def flush():
        nonlocal current_row
        if current_row:
            rows.append(current_row)
        current_row = None

    with open(input_file, encoding="utf-8") as f:
        for line in f:
            m = LOG_RE.match(line)
            if not m:
                continue

            body = m.group("body")
            km = KEYVAL_RE.match(body)
            if not km:
                continue

            key = km.group("key").strip()
            if key not in keys:
                continue

            found_keys.add(key)
            values = parse_values(km.group("values"))
            fields = keys[key]

            if len(values) < len(fields):
                continue

            # Check if this key's columns are already populated - if so, create new row
            if current_row and any(current_row[col] for col in key_cols[key]):
                flush()

            # Initialize row if needed
            if current_row is None:
                current_row = {c: "" for c in columns}

            for col, v in zip(key_cols[key], values):
                current_row[col] = v

    flush()
    return rows, found_keys


def remove_missing_columns(
    columns: list, key_cols: dict, rows: list, keys: dict, found_keys: set
) -> list:
    """Remove columns for keys not found in the log file."""
    missing_keys = set(keys.keys()) - found_keys
    if not missing_keys:
        return columns

    print("Warning: The following data fields were not found in the log file:")
    for key in sorted(missing_keys):
        print(f"  - {key}")

    columns_to_remove = set()
    for missing_key in missing_keys:
        columns_to_remove.update(key_cols[missing_key])

    columns = [col for col in columns if col not in columns_to_remove]

    for row in rows:
        for col in columns_to_remove:
            row.pop(col, None)

    return columns


def format_float_values(rows: list, columns: list) -> None:
    """Convert scientific notation to decimal format for better CSV compatibility."""
    for row in rows:
        for col in columns:
            if col in row and isinstance(row[col], float):
                row[col] = f"{row[col]:.10f}".rstrip("0").rstrip(".")


def write_csv(output_file: str, columns: list, rows: list) -> None:
    """Write data to CSV file."""
    with open(output_file, "w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input", required=True)
    ap.add_argument("-o", "--output", required=False)
    ap.add_argument("-c", "--config", default="config/roslog_to_csv.json")
    args = ap.parse_args()

    # If no output name is given, use input filename (without extension) + .csv
    if args.output is None:
        base_name = os.path.basename(args.input)
        name_without_ext = os.path.splitext(base_name)[0]
        args.output = name_without_ext + ".csv"

    cfg = load_config(args.config)
    keys = cfg["keys"]

    columns, key_cols = prepare_columns(keys)
    rows, found_keys = parse_log_file(args.input, keys, key_cols, columns)
    columns = remove_missing_columns(columns, key_cols, rows, keys, found_keys)
    format_float_values(rows, columns)
    write_csv(args.output, columns, rows)


if __name__ == "__main__":
    main()
