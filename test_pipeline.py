#!/usr/bin/env python3
"""
Unit tests and Data Quality assertions for Transactional Lakehouse Pipeline
"""

import unittest
import os
import tempfile
import json
from pipeline import process_pipeline

class TestDataPipeline(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.sample_data = [
            {"tx_id": "T1", "timestamp": "2025-01-01T12:00:00Z", "merchant_id": "M1", "merchant_name": "Shop A", "amount": 100.0, "status": "COMPLETED"},
            {"tx_id": "T1", "timestamp": "2025-01-01T12:00:00Z", "merchant_id": "M1", "merchant_name": "Shop A", "amount": 100.0, "status": "COMPLETED"}, # Duplicate
            {"tx_id": "T2", "timestamp": "2025-01-01T12:30:00Z", "merchant_id": "M1", "merchant_name": "Shop A", "amount": 50.0, "status": "REFUND"},
        ]
        self.input_file = os.path.join(self.test_dir, "input.json")
        with open(self.input_file, "w", encoding="utf-8") as f:
            json.dump(self.sample_data, f)

    def test_pipeline_deduplication_and_aggregation(self):
        process_pipeline(self.input_file, self.test_dir)

        # Check NoSQL archive
        nosql_path = os.path.join(self.test_dir, "nosql_archive_mongodb_feed.json")
        self.assertTrue(os.path.exists(nosql_path))
        with open(nosql_path, "r", encoding="utf-8") as f:
            archived = json.load(f)
        self.assertEqual(len(archived), 2, "Duplicate transaction T1 should have been filtered out")

        # Check SQL aggregation
        sql_path = os.path.join(self.test_dir, "sql_dw_merchant_hourly.csv")
        self.assertTrue(os.path.exists(sql_path))

    def tearDown(self):
        import shutil
        shutil.rmtree(self.test_dir)

if __name__ == "__main__":
    unittest.main()
