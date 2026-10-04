"""Isolated Spark integration tests; never touches the project datasets."""
import sys
import tempfile
import unittest
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, '/workspace/jobs')
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'jobs'))
from bronze_ingestion import BronzeIngestor


class BronzeIngestionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[2]')
                     .appName('bronze-incremental-contract')
                     .config('spark.sql.shuffle.partitions', '2').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='bronze-incremental-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.destination = self.root / 'bronze'
        self.config = {
            'sources': {'lineitem': {
                'path': str(self.destination), 'format': 'parquet',
                'primary_key': ['l_orderkey', 'l_linenumber'],
                'columns': {'l_orderkey': 'long', 'l_linenumber': 'integer', 'amount': 'string'},
            }},
            'ingestion': {'mode': 'incremental', 'on_existing_change': 'fail',
                          'source_system': 'ERP_LEGACY_TPCH',
                          'partition_by': {'orders': ['o_orderstatus']}},
        }
        self.config_path = self.root / 'config.yml'
        self.save_config()
        self.schema = 'l_orderkey long, l_linenumber int, amount string, extra string'
        self.serial = 0

    def save_config(self):
        self.config_path.write_text(yaml.safe_dump(self.config))

    def run_rows(self, rows, schema=None, table='lineitem'):
        self.serial += 1
        source = str(self.root / f'source-{self.serial}')
        self.spark.createDataFrame(rows, schema or self.schema).write.parquet(source)
        return BronzeIngestor(self.spark, self.config_path).run(table, source_path=source)

    def output(self):
        return self.spark.read.parquet(str(self.destination))

    def files(self):
        return {str(p): p.stat().st_size for p in self.destination.rglob('*') if p.is_file()}

    def legacy_frame(self, rows, schema):
        return (self.spark.createDataFrame(rows, schema)
                .withColumn('_ingestion_timestamp', F.current_timestamp())
                .withColumn('_source_system', F.lit('ERP_LEGACY_TPCH'))
                .withColumn('_execution_id', F.lit('legacy-run')))

    def test_incremental_preserves_full_schema_and_audit(self):
        first = (1, 1, '10.00', 'raw detail')
        result = self.run_rows([first, first])
        self.assertEqual(result['source_rows'], 2)
        self.assertEqual(result['deduplicated_rows'], 1)
        self.assertEqual(result['new_rows'], 1)
        original = self.output().collect()
        self.assertEqual(set(self.output().columns),
                         {'l_orderkey', 'l_linenumber', 'amount', 'extra',
                          '_ingestion_timestamp', '_source_system', '_execution_id'})
        self.assertEqual(self.output().schema['l_orderkey'].dataType.simpleString(), 'bigint')
        before = self.files()
        result = self.run_rows([first])
        self.assertEqual(result['new_rows'], 0)
        self.assertEqual(self.files(), before)
        self.assertEqual(self.output().collect(), original)
        result = self.run_rows([first, (1, 2, '20.00', 'new detail')])
        self.assertEqual(result['new_rows'], 1)
        self.assertEqual(self.output().count(), 2)
        self.assertEqual(self.output().where('l_linenumber = 1').collect(), original)

    def test_rejected_batches_leave_existing_files_intact(self):
        row = (1, 1, '10.00', 'raw detail')
        self.run_rows([row])
        before = self.files()
        for label, rows, schema in [
            ('update', [(1, 1, '11.00', 'raw detail'), (2, 1, '5', 'new')], None),
            ('null key', [(None, 1, '10', 'x')], None),
            ('conflicting input', [(2, 1, '10', 'x'), (2, 1, '11', 'x')], None),
            ('schema drift', [('2', 1, '10', 'x')],
             'l_orderkey string, l_linenumber int, amount string, extra string'),
        ]:
            with self.subTest(label=label):
                with self.assertRaises(ValueError):
                    self.run_rows(rows, schema)
                self.assertEqual(self.files(), before)
        result = self.run_rows([])
        self.assertEqual(result['new_rows'], 0)
        self.assertEqual(self.files(), before)

    def test_existing_duplicate_keys_are_rejected(self):
        row = (1, 1, '10', 'x')
        self.legacy_frame([row, row], self.schema).write.parquet(str(self.destination))
        before = self.files()
        with self.assertRaises(ValueError):
            self.run_rows([(2, 1, '20', 'y')])
        self.assertEqual(self.files(), before)

    def test_legacy_partitioned_orders_remain_readable(self):
        schema = 'o_orderkey long, o_orderstatus string, extra string'
        self.config['sources'] = {'orders': {
            'path': str(self.destination), 'format': 'parquet',
            'primary_key': ['o_orderkey'],
            'columns': {'o_orderkey': 'long', 'o_orderstatus': 'string'},
        }}
        self.save_config()
        self.legacy_frame([(1, 'O', 'old')], schema).write.partitionBy(
            'o_orderstatus').parquet(str(self.destination))
        result = self.run_rows([(1, 'O', 'old'), (2, 'F', 'new')], schema, 'orders')
        self.assertEqual(result['new_rows'], 1)
        self.assertEqual({(r.o_orderkey, r.o_orderstatus) for r in self.output().collect()},
                         {(1, 'O'), (2, 'F')})


if __name__ == '__main__':
    unittest.main(verbosity=2)
