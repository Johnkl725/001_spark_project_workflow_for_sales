"""Integration contract: run with Spark 3.5 and the YAML Delta JVM connector."""
import copy
import json
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobs"))
from silver_processor import SilverProcessor


class SilverContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="silver-contract-")
        cls.directory = Path(cls.temp.name)
        cls.config = yaml.safe_load((ROOT / "jobs/silver_config.yml").read_text())
        cls.config["spark"]["master"] = "local[2]"
        cls.config["spark"]["configs"]["spark.sql.shuffle.partitions"] = "2"
        for name, spec in cls.config["sources"].items():
            spec["path"] = str(cls.directory / name)
        cls.config["output"]["path"] = str(cls.directory / "silver")
        cls.config_path = cls.directory / "config.yml"
        cls.config_path.write_text(yaml.safe_dump(cls.config))
        cls.processor = SilverProcessor(cls.config_path)
        cls.spark = cls.processor.spark
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.rows = {
            "orders": [[str(i), "10", "2025-01-02", status, "100.01"]
                       for i, status in enumerate(["O", "F", "P"], 1)],
            "lineitem": [[str(i), "1", "20", "2", "100.01", "0.07", "0.18"]
                         for i in range(1, 4)],
            "customer": [["10", "Customer", "BUILDING"]],
        }

    @classmethod
    def tearDownClass(cls):
        cls.processor.close()
        cls.temp.cleanup()

    def write_rows(self, rows, names=None):
        for name in names or rows:
            spec = self.config["sources"][name]
            schema = ", ".join(f"{column} string" for column in spec["columns"])
            self.spark.createDataFrame(rows[name], schema).write.mode("overwrite").parquet(spec["path"])

    def versions(self):
        return sorted((self.directory / "silver/_delta_log").glob("*.json"))

    def test_contract_and_failure_atomicity(self):
        self.write_rows(self.rows)
        self.processor.run()
        output = self.spark.read.format("delta").load(self.config["output"]["path"])
        self.assertEqual(output.count(), 3)
        self.assertEqual({r.order_status for r in output.collect()}, {"Open", "Finished", "Pending"})
        self.assertEqual({r.net_revenue for r in output.collect()}, {Decimal("109.75")})
        for name, spec in self.config["output"]["columns"].items():
            self.assertEqual(output.schema[name].dataType.simpleString(),
                             {"long": "bigint", "integer": "int"}.get(spec["type"], spec["type"]))
        metadata = [json.loads(line)["metaData"] for line in self.versions()[0].read_text().splitlines()
                    if "metaData" in json.loads(line)][0]
        self.assertEqual(metadata["partitionColumns"], ["order_year", "order_month"])
        self.processor.run()
        self.assertEqual(self.spark.read.format("delta").load(self.config["output"]["path"]).count(), 3)
        good_versions = self.versions()
        cases = [
            ("bad_date", "orders", 0, 2, "2025-02-30"),
            ("unknown_status", "orders", 0, 3, "X"),
            ("fractional_quantity", "lineitem", 0, 3, "2.5"),
            ("lossy_financial_string", "lineitem", 0, 4, "100.001"),
            ("null", "lineitem", 0, 4, None),
            ("orphan", "lineitem", 0, 0, "999"),
            ("overflow", "lineitem", 0, 4, "10000000000000.00"),
            ("revenue_overflow", "lineitem", 0, 4, "9999999999999.99"),
        ]
        for label, name, row, column, value in cases:
            with self.subTest(label=label):
                bad = copy.deepcopy(self.rows)
                bad[name][row][column] = value
                self.write_rows(bad, [name])
                with self.assertRaises(Exception):
                    self.processor.run()
                self.assertEqual(self.versions(), good_versions)
                self.write_rows(self.rows, [name])
        with self.subTest(label="duplicate"):
            bad = copy.deepcopy(self.rows)
            bad["orders"].append(bad["orders"][0])
            self.write_rows(bad, ["orders"])
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                self.processor.run()
            self.assertEqual(self.versions(), good_versions)
        self.assertEqual(self.spark.read.format("delta").load(self.config["output"]["path"]).count(), 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
