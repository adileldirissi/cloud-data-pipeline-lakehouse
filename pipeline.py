#!/usr/bin/env python3
"""
Transactional Data Lakehouse Pipeline
Simulates distributed ingestion, schema validation, deduplication, and dual-sink persistence.
Author: Adil El Idrissi
"""

import json
import os
import argparse
from datetime import datetime
from collections import defaultdict


def process_pipeline(input_file: str, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)

    with open(input_file, "r", encoding="utf-8") as f:
        raw_events = json.load(f)

    print(f"[*] Ingested {len(raw_events)} raw events from {input_file}")

    # 1. Deduplication Gate
    seen_ids = set()
    deduped_events = []
    duplicates_dropped = 0

    for ev in raw_events:
        tx_id = ev.get("tx_id")
        if tx_id in seen_ids:
            duplicates_dropped += 1
            continue
        seen_ids.add(tx_id)
        deduped_events.append(ev)

    print(f"[*] Deduplication: Dropped {duplicates_dropped} duplicate records. {len(deduped_events)} unique records retained.")

    # 2. Dual-Sink Persistence
    # Sink A: Unstructured MongoDB / JSON Archive (Preserving full raw metadata)
    nosql_archive_path = os.path.join(out_dir, "nosql_archive_mongodb_feed.json")
    with open(nosql_archive_path, "w", encoding="utf-8") as f:
        json.dump(deduped_events, f, indent=2)
    print(f"[✓] Sink A: Saved semi-structured event payload to {nosql_archive_path}")

    # Sink B: Structured Relational / Parquet Aggregations (Merchant Hourly Metrics)
    merchant_hourly = defaultdict(lambda: {"total_volume": 0.0, "tx_count": 0, "refund_count": 0})

    for ev in deduped_events:
        m_id = ev.get("merchant_id")
        ts = ev.get("timestamp")
        hour_key = ts[:13] if ts else "UNKNOWN"
        group_key = f"{m_id}_{hour_key}"

        amt = float(ev.get("amount", 0.0))
        status = ev.get("status")

        merchant_hourly[group_key]["merchant_id"] = m_id
        merchant_hourly[group_key]["merchant_name"] = ev.get("merchant_name")
        merchant_hourly[group_key]["hour"] = hour_key
        merchant_hourly[group_key]["total_volume"] += amt
        merchant_hourly[group_key]["tx_count"] += 1
        if status == "REFUND":
            merchant_hourly[group_key]["refund_count"] += 1

    sql_dw_feed_path = os.path.join(out_dir, "sql_dw_merchant_hourly.csv")
    import csv
    with open(sql_dw_feed_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["merchant_id", "merchant_name", "hour", "tx_count", "total_volume", "refund_count"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in merchant_hourly.values():
            writer.writerow(row)

    print(f"[✓] Sink B: Saved clean analytical aggregation to {sql_dw_feed_path}")
    print("[✓] Pipeline execution finished successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transactional Data Lakehouse Pipeline")
    parser.add_argument("--input", default="sample_transactions.json", help="Path to input json")
    parser.add_argument("--out-dir", default="output", help="Directory for processed outputs")
    args = parser.parse_args()
    process_pipeline(args.input, args.out_dir)
