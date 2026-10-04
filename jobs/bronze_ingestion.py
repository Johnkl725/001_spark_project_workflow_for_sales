"""Insert-only incremental Bronze ingestion for a single-writer Parquet pipeline.

Auditing is not CDC: keys identify existing rows, payload comparisons detect changes.
Source snapshots still require a scan. Parquet append is not a Delta transaction.
"""
from __future__ import annotations

from datetime import datetime, timezone
from functools import reduce
from pathlib import Path
import uuid

import yaml
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F


AUDIT_COLUMNS = ("_ingestion_timestamp", "_source_system", "_execution_id")


class BronzeIngestor:
    def __init__(self, spark: SparkSession, config_path: str | Path):
        self.spark = spark
        self.config_path = Path(config_path).resolve()
        self.config = yaml.safe_load(self.config_path.read_text(encoding="utf-8"))

    def _path(self, value: str) -> str:
        if "://" in value or value.startswith("dbfs:"):
            return value
        path = Path(value)
        return str((path if path.is_absolute() else self.config_path.parent / path).resolve())

    def _exists(self, value: str) -> bool:
        path = self.spark._jvm.org.apache.hadoop.fs.Path(value)
        return path.getFileSystem(self.spark._jsc.hadoopConfiguration()).exists(path)

    @staticmethod
    def _assert_empty(frame, message):
        if frame.limit(1).count():
            raise ValueError(message)

    def run(self, table: str, source_path: str | None = None,
            mode: str | None = None) -> dict:
        settings = self.config["ingestion"]
        spec = self.config["sources"][table]
        mode = mode or settings["mode"]
        if mode not in ("incremental", "snapshot"):
            raise ValueError("ingestion.mode must be incremental or snapshot")
        if settings["on_existing_change"] != "fail":
            raise ValueError("Parquet insert-only ingestion supports on_existing_change: fail")
        source_path = self._path(source_path or settings["source_paths"][table])
        destination = self._path(spec["path"])
        origin = source_path.replace("\\", "/").rstrip("/")
        target = destination.replace("\\", "/").rstrip("/")
        if origin == target or origin.startswith(target + "/") or target.startswith(origin + "/"):
            raise ValueError("Source and Bronze paths must not overlap")
        if spec["format"] != "parquet":
            raise ValueError("This ingestor requires Bronze format parquet")
        keys = spec["primary_key"]
        if not keys:
            raise ValueError("A primary key is required")
        source = self.spark.read.parquet(source_path)
        missing = (set(spec["columns"]) | set(keys)) - set(source.columns)
        if missing:
            raise ValueError(f"Missing Source columns: {sorted(missing)}")
        if set(AUDIT_COLUMNS).intersection(source.columns):
            raise ValueError("Source contains reserved audit columns")
        self._assert_empty(source.where(reduce(lambda a, b: a | b,
                           [F.col(key).isNull() for key in keys])), "Null primary key in Source")
        # Drop exact duplicates; a conflicting payload has no trustworthy ERP version.
        payload = source.dropDuplicates(source.columns).persist(StorageLevel.MEMORY_AND_DISK)
        pending = None
        try:
            source_count = source.count()
            unique_count = payload.count()
            self._assert_empty(payload.groupBy(*keys).count().where(F.col("count") > 1),
                               "Conflicting duplicate primary keys in Source")
            partition_columns = []
            existing = None
            if self._exists(destination):
                existing = self.spark.read.parquet(destination)
                # Preserve the legacy orders partition layout when appending.
                configured_partitions = settings.get("partition_by", {}).get(table, [])
                partition_columns = configured_partitions
                expected = set(source.columns) | set(AUDIT_COLUMNS)
                if set(existing.columns) != expected:
                    raise ValueError("Bronze schema columns differ from Source plus audit columns")
                for field in source.schema:
                    if existing.schema[field.name].dataType != field.dataType:
                        raise ValueError(f"Bronze schema drift for {field.name}; refusing implicit cast")
                self._assert_empty(existing.where(reduce(lambda a, b: a | b,
                                   [F.col(key).isNull() for key in keys])), "Null primary key in Bronze")
                self._assert_empty(existing.groupBy(*keys).count().where(F.col("count") > 1),
                                   "Duplicate primary key in existing Bronze")
            else:
                partition_columns = settings.get("partition_by", {}).get(table, [])
            if not set(partition_columns).issubset(source.columns):
                raise ValueError("Bronze partition columns must exist in Source")
            if mode == "incremental" and existing is not None:
                left, right = payload.alias("incoming"), existing.alias("stored")
                key_match = reduce(lambda a, b: a & b,
                                   [left[key] == right[key] for key in keys])
                same_payload = reduce(lambda a, b: a & b,
                                      [left[col].eqNullSafe(right[col]) for col in source.columns])
                self._assert_empty(left.join(right, key_match, "inner").where(~same_payload),
                                   "Existing key has changed payload; Parquet cannot upsert safely")
                pending = payload.join(existing.select(*keys), keys, "left_anti")
            else:
                pending = payload
            pending = pending.persist(StorageLevel.MEMORY_AND_DISK)
            new_count = pending.count()
            report = {"table": table, "mode": mode, "source_rows": source_count,
                      "deduplicated_rows": unique_count, "new_rows": new_count,
                      "existing_rows": unique_count - new_count, "path": destination}
            if new_count == 0:
                report["status"] = "no_op"
                return report
            execution_id = str(uuid.uuid4())
            audited = (pending.withColumn("_ingestion_timestamp", F.lit(datetime.now(timezone.utc)))
                       .withColumn("_source_system", F.lit(settings["source_system"]))
                       .withColumn("_execution_id", F.lit(execution_id)))
            writer = audited.write.mode("append" if mode == "incremental" else "overwrite")
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            writer.parquet(destination)
            self.spark.catalog.refreshByPath(destination)
            report.update(status="written", execution_id=execution_id)
            return report
        finally:
            if pending is not None:
                pending.unpersist()
            payload.unpersist()
