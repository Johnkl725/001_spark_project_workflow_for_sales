"""Strict, YAML-driven TPC-H Bronze to Silver pipeline.

Requires Spark's Delta Lake JVM connector (configured in YAML or on the cluster).
The Python ``deltalake`` package alone does not provide that connector.
"""
from __future__ import annotations

import argparse
import logging
from functools import reduce
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DecimalType, IntegralType, NumericType


class SilverProcessor:
    """Build and persist a validated Silver table without owning a supplied session."""

    def __init__(self, config_path: str | Path, spark: SparkSession | None = None):
        self.config_path = Path(config_path).resolve()
        with self.config_path.open(encoding="utf-8") as stream:
            self.config: dict[str, Any] = yaml.safe_load(stream)
        self._validate_config()
        self._owns_spark = spark is None
        self.spark = spark or self._create_spark()
        # Invalid conversions, arithmetic overflow and decimal overflow must fail.
        self.spark.conf.set("spark.sql.ansi.enabled", "true")
        self.spark.conf.set("spark.sql.storeAssignmentPolicy", "ANSI")
        self.spark.conf.set("spark.sql.decimalOperations.allowPrecisionLoss", "false")

    def _validate_config(self) -> None:
        cfg = self.config
        if not isinstance(cfg, dict):
            raise ValueError("The YAML root must be a mapping")
        for key in ("sources", "joins", "business_rules", "output", "quality"):
            if key not in cfg:
                raise ValueError(f"Missing YAML section: {key}")
        if "lineitem" not in cfg["sources"]:
            raise ValueError("sources.lineitem is required as the base fact table")
        output = cfg["output"]
        if output.get("format") != "delta":
            raise ValueError("output.format must be delta")
        if output.get("mode") not in ("overwrite", "error", "errorifexists", "merge"):
            raise ValueError("Use overwrite, merge, or errorifexists; append would duplicate batch facts")
        names: set[str] = set()
        for name, source in cfg["sources"].items():
            if not source.get("columns") or not source.get("primary_key"):
                raise ValueError(f"Source {name} requires columns and primary_key")
            duplicates = names.intersection(source["columns"])
            if duplicates:
                raise ValueError(f"Ambiguous source columns: {sorted(duplicates)}")
            names.update(source["columns"])
            if not set(source["primary_key"]).issubset(source["columns"]):
                raise ValueError(f"Primary key of {name} is absent from selected columns")
        for name, spec in output["columns"].items():
            if not isinstance(spec, dict) or not spec.get("source") or not spec.get("type"):
                raise ValueError(f"Output column {name} requires source and type")
        partitioning = output["partitioning"]
        for key in ("year_column", "month_column"):
            if partitioning[key] not in output["columns"]:
                raise ValueError(f"Partition column {partitioning[key]} must be in output.columns")
        if not set(cfg["quality"]["required_columns"]).issubset(output["columns"]):
            raise ValueError("quality.required_columns must refer to final output columns")
        def normalized(value: str) -> str:
            if "://" in value or value.startswith("dbfs:"):
                return value.rstrip("/")
            return str(Path(self._path(value)).resolve()).replace("\\", "/").rstrip("/").casefold()

        destination = normalized(output["path"])
        for name, source in cfg["sources"].items():
            origin = normalized(source["path"])
            if (destination == origin or destination.startswith(origin + "/")
                    or origin.startswith(destination + "/")):
                raise ValueError(f"Silver output overlaps Bronze source {name}; choose separate paths")

    def _create_spark(self) -> SparkSession:
        settings = self.config.get("spark", {})
        builder = SparkSession.builder.appName(settings.get("app_name", "TPC-H Silver"))
        if settings.get("master"):
            builder = builder.master(settings["master"])
        for key, value in settings.get("configs", {}).items():
            builder = builder.config(key, value)
        return builder.getOrCreate()

    def _path(self, value: str) -> str:
        # Preserve distributed storage URIs; local paths are relative to the YAML.
        if "://" in value or value.startswith("dbfs:"):
            return value
        path = Path(value)
        return str(path if path.is_absolute() else self.config_path.parent / path)

    @staticmethod
    def _assert_empty(frame: DataFrame, message: str) -> None:
        if frame.limit(1).count():
            raise ValueError(message)

    def _cast_columns(self, frame: DataFrame, specs: dict[str, dict[str, str]],
                      allow_revenue_rounding: bool = False) -> DataFrame:
        """Explicit casts plus rejection of lossy financial/integer conversions."""
        missing = {spec["source"] for spec in specs.values()} - set(frame.columns)
        if missing:
            raise ValueError(f"Missing input columns: {sorted(missing)}")
        result = frame.select(*[
            F.col(spec["source"]).cast(spec["type"]).alias(name)
            for name, spec in specs.items()
        ])
        # Obtain resolved types from Spark, never maintain Python type definitions.
        resolved = {field.name: field.dataType for field in result.schema.fields}
        checks = []
        for name, spec in specs.items():
            dtype = resolved[name]
            original = F.col(spec["source"])
            converted = original.cast(spec["type"])
            if isinstance(dtype, IntegralType):
                input_type = frame.schema[spec["source"]].dataType
                if isinstance(input_type, NumericType):
                    checks.append(original.isNotNull() & (original != converted.cast(input_type)))
                else:
                    # Fractional text must not silently truncate in an integer cast.
                    checks.append(original.isNotNull() & original.rlike(r"[.][0-9]*[1-9]|[eE]"))
            elif isinstance(dtype, DecimalType):
                input_type = frame.schema[spec["source"]].dataType
                may_round = (allow_revenue_rounding and
                             spec["source"] == self.config["business_rules"]["revenue"]["output"])
                if not may_round:
                    if isinstance(input_type, NumericType):
                        checks.append(original.isNotNull() & (original != converted))
                    else:
                        # Extra trailing zeros are harmless; nonzero discarded digits are not.
                        pattern = rf"[.][0-9]{{{dtype.scale}}}[0-9]*[1-9]|[eE]"
                        checks.append(original.isNotNull() & original.rlike(pattern))
        if checks:
            self._assert_empty(frame.where(reduce(lambda a, b: a | b, checks)),
                               "A numeric cast would lose precision or truncate an integer")
        # Force cast evaluation now, including columns unused by downstream rules.
        nulls = reduce(lambda a, b: a | b, [F.col(name).isNull() for name in specs])
        self._assert_empty(result.where(nulls), "Null or invalid value in strictly typed columns")
        return result

    def _read_sources(self) -> dict[str, DataFrame]:
        frames = {}
        for name, spec in self.config["sources"].items():
            raw = self.spark.read.format(spec["format"]).load(self._path(spec["path"]))
            frame = self._cast_columns(raw, {
                col: {"source": col, "type": dtype} for col, dtype in spec["columns"].items()
            })
            duplicates = frame.groupBy(*spec["primary_key"]).count().where(F.col("count") > 1)
            self._assert_empty(duplicates, f"Duplicate primary key in Bronze source {name}")
            status = self.config["business_rules"]["order_status"]
            if status["column"] in frame.columns:
                self._assert_empty(frame.where(~F.col(status["column"]).isin(list(status["mapping"]))),
                                   f"Unknown order status in Bronze source {name}")
            frames[name] = frame
        return frames

    def _join_sources(self, frames: dict[str, DataFrame]) -> DataFrame:
        result = frames["lineitem"]
        joined = {"lineitem"}
        for join in self.config["joins"]:
            left, right = join["left"], join["right"]
            candidates = [name for name, frame in frames.items() if right in frame.columns and name not in joined]
            if left not in result.columns or len(candidates) != 1:
                raise ValueError(f"Invalid or ambiguous join: {join}")
            name = candidates[0]
            dimension = frames[name]
            self._assert_empty(dimension.groupBy(right).count().where(F.col("count") > 1),
                               f"Join key {right} is not unique in {name}")
            condition = result[left] == dimension[right]
            self._assert_empty(result.join(dimension, condition, "left_anti"),
                               f"Orphan foreign key {left} referencing {name}.{right}")
            result = result.join(dimension, condition, "inner")
            joined.add(name)
        if joined != set(frames):
            raise ValueError(f"Unjoined sources: {sorted(set(frames) - joined)}")
        return result

    def _business_rules(self, frame: DataFrame) -> DataFrame:
        rules = self.config["business_rules"]
        status = rules["order_status"]
        self._assert_empty(frame.where(~F.col(status["column"]).isin(list(status["mapping"]))),
                           "Unknown order status; update the YAML mapping explicitly")
        mapping = F.create_map(*[F.lit(value) for pair in status["mapping"].items() for value in pair])
        frame = frame.withColumn(status["column"], mapping[F.col(status["column"])])
        revenue = rules["revenue"]
        one = F.lit(1).cast(revenue["constant_type"])
        frame = frame.withColumn(revenue["output"],
                                 F.col(revenue["price"]) * (one - F.col(revenue["discount"]))
                                 * (one + F.col(revenue["tax"])))
        partitions = self.config["output"]["partitioning"]
        return frame.withColumn(partitions["year_column"], F.year(partitions["date_column"])) \
            .withColumn(partitions["month_column"], F.month(partitions["date_column"]))

    def run(self) -> DataFrame:
        """Validate, transform and write a complete deterministic batch snapshot."""
        transformed = self._business_rules(self._join_sources(self._read_sources()))
        output = self.config["output"]
        result = self._cast_columns(transformed, output["columns"], allow_revenue_rounding=True)
        required = self.config["quality"]["required_columns"]
        if required:
            self._assert_empty(result.where(reduce(lambda a, b: a | b,
                                                  [F.col(name).isNull() for name in required])),
                               "Required Silver column contains null")
        partitions = output["partitioning"]
        path = self._path(output["path"])
        
        if output.get("mode") == "merge":
            from delta.tables import DeltaTable
            if DeltaTable.isDeltaTable(self.spark, path):
                target_table = DeltaTable.forPath(self.spark, path)
                pk_cols = output.get("primary_key")
                if not pk_cols:
                    raise ValueError("primary_key must be defined in config output for merge mode.")
                merge_cond = " AND ".join([f"target.{col} = source.{col}" for col in pk_cols])
                
                target_table.alias("target").merge(
                    result.alias("source"),
                    merge_cond
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            else:
                result.write.format("delta").partitionBy(partitions["year_column"], partitions["month_column"]).save(path)
        else:
            writer = result.write.format("delta").mode(output.get("mode", "overwrite")) \
                .partitionBy(partitions["year_column"], partitions["month_column"])
            writer.save(path)
            
        logging.getLogger(__name__).info("Silver Delta table written to %s", path)
        return result

    def close(self) -> None:
        if self._owns_spark:
            self.spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", help="Path to silver_config.yml")
    args = parser.parse_args()
    processor = SilverProcessor(args.config)
    try:
        processor.run()
    finally:
        processor.close()


if __name__ == "__main__":
    main()
