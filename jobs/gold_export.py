"""Publish an immutable Gold release for SQL/Delta and optional Power BI CSV import.

current.json is the publication pointer. It changes only after every export succeeds.
It is not a transaction across multiple Power BI queries: refresh against one release.
"""
from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
from functools import reduce
import json
import os
from pathlib import Path
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


class GoldExporter:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def _local_path(self, value):
        if "://" in value or value.startswith("dbfs:"):
            raise ValueError("CSV publication requires a shared filesystem path, not a cloud URI")
        path = Path(value)
        if not path.is_absolute():
            raise ValueError("export.root and publish.current_manifest must be absolute paths")
        return path.resolve()

    def _validate_csv(self, name, frame, settings):
        conditions = []
        max_currency = Decimal(settings["max_currency_abs"])
        if not max_currency.is_finite() or max_currency <= 0:
            raise ValueError("export.max_currency_abs must be a positive finite decimal")
        for field in frame.schema:
            if isinstance(field.dataType, DecimalType):
                if field.dataType.scale > 4:
                    raise ValueError(f"{name}.{field.name} exceeds Power BI Fixed Decimal scale")
                # Infer the literal precision independently: the cap itself may
                # exceed the precision of a narrow, perfectly valid money column.
                limit = F.lit(max_currency)
                conditions.append(F.abs(F.col(field.name)) > limit)
        if conditions and frame.where(reduce(lambda a, b: a | b, conditions)).limit(1).count():
            raise ValueError(f"{name}: amount exceeds Power BI Fixed Decimal capacity")

    def publish(self, frames, report):
        settings = self.config["export"]
        manifest_path = self._local_path(self.config["publish"]["current_manifest"])
        root = self._local_path(settings["root"])
        # The manifest and its CSV-relative paths share the same consumer root.
        if manifest_path.parent != root:
            raise ValueError("publish.current_manifest must reside directly under export.root")
        manifest = deepcopy(report)
        manifest["source_version"] = report["source"]["version"]
        manifest["manifest_version"] = 1
        manifest["csv_enabled"] = bool(settings["enabled"])
        for name, frame in frames.items():
            manifest["tables"][name]["grain"] = self.config["tables"][name]["grain"]
            manifest["tables"][name]["schema"] = [
                {"name": field.name, "type": field.dataType.simpleString()}
                for field in frame.schema
            ]
        if settings["enabled"]:
            selected = settings["tables"]
            if len(selected) != len(set(selected)) or set(selected) - set(frames):
                raise ValueError("export.tables must contain unique, known Gold table names")
            partitions = settings["partitions"]
            if not isinstance(partitions, int) or partitions < 1:
                raise ValueError("export.partitions must be a positive integer")
            options = settings["csv_options"]
            if str(options.get("header")).lower() != "true" or options.get("delimiter") != ",":
                raise ValueError("Power Query contract requires CSV with header and comma delimiter")
            # Validate all currency columns before producing any CSV tables.
            for name in selected:
                self._validate_csv(name, frames[name], settings)
            for name in selected:
                relative = Path("releases") / report["release_id"] / name
                destination = root / relative
                # New immutable directories: no overwrite of a previously published release.
                frames[name].coalesce(partitions).write.mode("errorifexists") \
                    .options(**options).csv(str(destination))
                if not (destination / "_SUCCESS").is_file():
                    raise RuntimeError(f"CSV export did not complete: {name}")
                manifest["tables"][name]["csv_relative_path"] = relative.as_posix()
        root.mkdir(parents=True, exist_ok=True)
        # Keep the run manifest for replay and audit; switch the active pointer last.
        release = root / "releases" / report["release_id"]
        release.mkdir(parents=True, exist_ok=True)
        body = json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
        (release / "manifest.json").write_text(body, encoding="utf-8")
        temporary = root / f".current-{uuid.uuid4().hex}.tmp"
        try:
            with temporary.open("x", encoding="utf-8") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, manifest_path)
        finally:
            if temporary.exists():
                temporary.unlink()
        manifest["current_manifest"] = str(manifest_path)
        return manifest
