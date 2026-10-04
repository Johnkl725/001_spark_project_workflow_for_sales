"""Build validated business marts from a pinned Silver Delta snapshot.

Each run writes an immutable release. Publication is delegated to GoldExporter
only after every table is complete. Readers using the manifest see complete
releases; direct directory scans do not have this publication guarantee.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path
from typing import Any
from uuid import uuid4

import yaml
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F


class GoldProcessor:
    def __init__(self, config_path: str | Path, spark: SparkSession | None = None):
        self.config_path = Path(config_path).resolve()
        with self.config_path.open(encoding="utf-8") as stream:
            self.config: dict[str, Any] = yaml.safe_load(stream)
        for section in ("source", "output", "tables", "business_rules", "quality"):
            if section not in self.config:
                raise ValueError(f"Missing YAML section: {section}")
        if self.config["source"].get("format") != "delta":
            raise ValueError("Gold requires a Delta Silver source")
        self.config["source"]["path"] = self._path(self.config["source"]["path"])
        self.config["output"]["root"] = self._path(self.config["output"]["root"])
        if self.config.get("export", {}).get("root"):
            self.config["export"]["root"] = self._path(self.config["export"]["root"])
        if self.config.get("publish", {}).get("current_manifest"):
            self.config["publish"]["current_manifest"] = self._path(self.config["publish"]["current_manifest"])
        source = self.config["source"]["path"].replace("\\", "/").rstrip("/").casefold()
        target = self.config["output"]["root"].replace("\\", "/").rstrip("/").casefold()
        if source == target or source.startswith(target + "/") or target.startswith(source + "/"):
            raise ValueError("Gold and Silver paths must not overlap")
        if self.config.get("export", {}).get("enabled"):
            export = self.config["export"]["root"].replace("\\", "/").rstrip("/").casefold()
            for label, data_path in (("Silver", source), ("Gold Delta", target)):
                if export == data_path or export.startswith(data_path + "/") or data_path.startswith(export + "/"):
                    raise ValueError(f"CSV export overlaps {label}; choose separate paths")
        self._owns_spark = spark is None
        settings = self.config.get("spark", {})
        builder = SparkSession.builder.appName(settings.get("app_name", "TPC-H Gold"))
        if settings.get("master"):
            builder = builder.master(settings["master"])
        for key, value in settings.get("configs", {}).items():
            builder = builder.config(key, value)
        self.spark = spark or builder.getOrCreate()
        self.spark.conf.set("spark.sql.ansi.enabled", "true")
        self.spark.conf.set("spark.sql.storeAssignmentPolicy", "ANSI")
        self.spark.conf.set("spark.sql.decimalOperations.allowPrecisionLoss", "false")
        self._source: DataFrame | None = None

    def _path(self, value: str) -> str:
        if "://" in value or value.startswith("dbfs:"):
            return value
        path = Path(value)
        return str((path if path.is_absolute() else self.config_path.parent / path).resolve())

    @staticmethod
    def _assert_empty(frame: DataFrame, message: str) -> None:
        if frame.limit(1).count():
            raise ValueError(message)

    def _validate_source(self, frame: DataFrame) -> None:
        for name, dtype in self.config["source"]["columns"].items():
            if name not in frame.columns:
                raise ValueError(f"Missing Silver column: {name}")
            expected = self.spark._jsparkSession.sessionState().sqlParser().parseDataType(dtype).catalogString()
            actual = frame.schema[name].dataType.simpleString()
            if expected != actual:
                raise ValueError(f"Silver type mismatch {name}: expected {expected}, found {actual}")
        required = self.config["quality"]["required_columns"]
        if required:
            self._assert_empty(frame.where(reduce(lambda a, b: a | b,
                               [F.col(c).isNull() for c in required])), "Null required Silver value")
        if not frame.limit(1).count():
            raise ValueError("Silver is empty; no Gold release was published")
        self._assert_empty(frame.groupBy("order_id", "line_number").count().where("count > 1"),
                           "Duplicate Silver order line")
        self._assert_empty(frame.where((F.col("quantity") <= 0) | (F.col("base_price") < 0)
                           | (F.col("order_total") < 0) | (F.col("net_revenue") < 0)
                           | (~F.col("discount_rate").between(0, 1))
                           | (~F.col("tax_rate").between(0, 1))), "Invalid business measure")
        self._assert_empty(frame.where((F.year("order_date") != F.col("order_year"))
                           | (F.month("order_date") != F.col("order_month"))), "Silver date partitions disagree")
        attributes = ["order_date", "customer_id", "order_status", "order_total"]
        self._assert_empty(frame.groupBy("order_id").agg(
            F.countDistinct(F.struct(*attributes)).alias("variants")).where("variants > 1"),
            "Inconsistent attributes within one order")
        self._assert_empty(frame.groupBy("customer_id").agg(
            F.countDistinct(F.struct("customer_name", "market_segment")).alias("variants"))
            .where("variants > 1"), "Inconsistent customer attributes")
        one = F.lit(1).cast(self.config["business_rules"]["constant_type"])
        expected = (F.col("base_price") * (one - F.col("discount_rate"))
                    * (one + F.col("tax_rate"))).cast(self.config["source"]["columns"]["net_revenue"])
        self._assert_empty(frame.where(F.col("net_revenue") != expected),
                           "Silver net_revenue disagrees with its financial rule")

    def _cast(self, name: str, frame: DataFrame) -> DataFrame:
        columns = self.config["tables"][name]["columns"]
        return frame.select(*[F.col(column).cast(dtype).alias(column)
                              for column, dtype in columns.items()])

    def build_tables(self, source: DataFrame | None = None) -> dict[str, DataFrame]:
        """Build marts; callers may supply a fixture without writing any data."""
        frame = source if source is not None else self._source
        if frame is None:
            raise ValueError("Supply a Silver DataFrame or call run()")
        self._validate_source(frame)
        rules = self.config["business_rules"]
        one = F.lit(1).cast(rules["constant_type"])
        enriched = frame.withColumn("date_key", F.date_format("order_date", "yyyyMMdd")) \
            .withColumn("revenue_before_tax", (F.col("base_price") * (one - F.col("discount_rate"))).cast(rules["money_type"])) \
            .withColumn("discount_amount", F.col("base_price") - F.col("revenue_before_tax")) \
            .withColumn("tax_amount", F.col("net_revenue") - F.col("revenue_before_tax"))
        sales = self._cast("fact_sales", enriched)
        self._assert_empty(sales.where(F.col("net_revenue") !=
                           F.col("revenue_before_tax") + F.col("tax_amount")), "Line revenue reconciliation failed")
        money = ["base_price", "discount_amount", "revenue_before_tax", "tax_amount", "net_revenue"]
        order_keys = ["order_id", "date_key", "customer_id", "order_status", "order_total", "order_year", "order_month"]
        orders = enriched.groupBy(*order_keys).agg(F.count("*").alias("line_count"),
                   F.sum("quantity").alias("quantity"), *[F.sum(c).alias(c) for c in money])
        bounds = frame.agg(F.min("order_date").alias("min"), F.max("order_date").alias("max")).first()
        dates = self.spark.range(1).select(F.explode(F.sequence(
            F.to_date(F.lit(f"{bounds['min'].year}-01-01")),
            F.to_date(F.lit(f"{bounds['max'].year}-12-31")), F.expr("INTERVAL 1 DAY"))).alias("date"))
        dates = dates.select("date", F.date_format("date", "yyyyMMdd").alias("date_key"),
                  F.year("date").alias("year"), F.quarter("date").alias("quarter"),
                  F.month("date").alias("month"), F.date_format("date", "MMMM").alias("month_name"),
                  F.date_format("date", "yyyy-MM").alias("year_month"),
                  F.dayofmonth("date").alias("day_of_month"),
                  (F.pmod(F.dayofweek("date") + F.lit(5), F.lit(7)) + F.lit(1)).alias("day_of_week"))
        def aggregate(keys: list[str]) -> DataFrame:
            return enriched.groupBy(*keys).agg(F.count("*").alias("line_count"),
                F.countDistinct("order_id").alias("order_count"),
                F.countDistinct("customer_id").alias("customer_count"), F.sum("quantity").alias("quantity"),
                *[F.sum(c).alias(c) for c in money]) \
                .withColumn("month_start_date", F.make_date("order_year", "order_month", F.lit(1))) \
                .withColumn("date_key", F.date_format("month_start_date", "yyyyMMdd"))
        tables = {
            "fact_sales": sales,
            "fact_orders": orders,
            "dim_date": dates,
            "dim_customer": frame.select("customer_id", "customer_name", "market_segment").distinct(),
            "dim_product": frame.select("product_id").distinct(),
            "dim_order_status": frame.select("order_status").distinct(),
            "dim_market_segment": frame.select("market_segment").distinct(),
            "agg_sales_monthly": aggregate(["order_year", "order_month", "market_segment", "order_status"]),
            "agg_customer_monthly": aggregate(["order_year", "order_month", "customer_id"]),
        }
        if set(tables) != set(self.config["tables"]):
            raise ValueError("YAML tables must match the supported business marts")
        result = {name: self._cast(name, data) for name, data in tables.items()}
        for name, data in result.items():
            spec = self.config["tables"][name]
            self._assert_empty(data.where(reduce(lambda a, b: a | b,
                               [F.col(c).isNull() for c in spec["columns"]])), f"Null Gold value in {name}")
            self._assert_empty(data.groupBy(*spec["primary_key"]).count().where("count > 1"),
                               f"Duplicate Gold primary key in {name}")
            if all(c in data.columns for c in ("net_revenue", "revenue_before_tax", "tax_amount")):
                self._assert_empty(data.where(F.col("net_revenue") !=
                                   F.col("revenue_before_tax") + F.col("tax_amount")),
                                   f"Revenue reconciliation failed in {name}")
                self._assert_empty(data.where(F.col("base_price") !=
                                   F.col("revenue_before_tax") + F.col("discount_amount")),
                                   f"Discount reconciliation failed in {name}")
        return result

    def run(self) -> dict[str, Any]:
        path = self.config["source"]["path"]
        escaped_path = path.replace("`", "``")
        version = int(self.spark.sql(f"DESCRIBE HISTORY delta.`{escaped_path}`")
                      .orderBy(F.col("version").desc()).first()["version"])
        self._source = self.spark.read.format("delta").option("versionAsOf", version).load(path) \
            .persist(StorageLevel.MEMORY_AND_DISK)
        try:
            frames = self.build_tables()
            release_id = uuid4().hex
            mode = self.config["output"].get("mode", "overwrite")
            if mode == "merge":
                root = self.config["output"]["root"].rstrip("/\\")
            else:
                root = self.config["output"]["root"].rstrip("/\\") + "/releases/" + release_id
                
            report = {"release_id": release_id, "created_at": datetime.now(timezone.utc).isoformat(),
                      "source": {"path": path, "version": version}, "tables": {}}
            for name, data in frames.items():
                destination = root + "/" + name
                partitions = self.config["tables"][name].get("partition_by", [])
                
                if mode == "merge":
                    from delta.tables import DeltaTable
                    if DeltaTable.isDeltaTable(self.spark, destination):
                        target_table = DeltaTable.forPath(self.spark, destination)
                        pk_cols = self.config["tables"][name].get("primary_key")
                        if not pk_cols:
                            raise ValueError(f"primary_key missing for table {name}")
                        merge_cond = " AND ".join([f"target.{c} = source.{c}" for c in pk_cols])
                        target_table.alias("target").merge(
                            data.alias("source"), merge_cond
                        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                    else:
                        writer = data.write.format("delta")
                        if partitions:
                            writer = writer.partitionBy(*partitions)
                        writer.save(destination)
                else:
                    writer = data.write.format("delta").mode("errorifexists")
                    if partitions:
                        writer = writer.partitionBy(*partitions)
                    writer.save(destination)
                    
                report["tables"][name] = {"path": destination, "version": 0, "rows": data.count()}
            try:
                from .gold_export import GoldExporter
            except ImportError:
                from gold_export import GoldExporter
            return GoldExporter(self.spark, self.config).publish(frames, report)
        finally:
            self._source.unpersist()
            self._source = None

    def close(self) -> None:
        if self._owns_spark:
            self.spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", help="Path to gold_config.yml")
    processor = GoldProcessor(parser.parse_args().config)
    try:
        print(processor.run())
    finally:
        processor.close()


if __name__ == "__main__":
    main()
