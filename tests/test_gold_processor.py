"""Gold business and publication contract; uses only temporary local Delta data."""
import copy
import csv
import json
import sys
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'jobs'))
sys.path.insert(0, '/workspace/jobs')
from gold_processor import GoldProcessor
from gold_export import GoldExporter


class GoldContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(prefix='gold-contract-')
        cls.directory = Path(cls.tmp.name)
        cls.config = yaml.safe_load((Path('/workspace/jobs/gold_config.yml')
                                    if Path('/workspace/jobs/gold_config.yml').exists()
                                    else ROOT / 'jobs/gold_config.yml').read_text())
        cls.config['spark']['master'] = 'local[2]'
        cls.config['spark']['configs']['spark.sql.shuffle.partitions'] = '2'
        cls.config['source']['path'] = str(cls.directory / 'silver')
        cls.config['output']['root'] = str(cls.directory / 'gold')
        cls.config['export']['root'] = str(cls.directory / 'exports')
        cls.config['publish']['current_manifest'] = str(cls.directory / 'exports/current.json')
        cls.config_path = cls.directory / 'config.yml'
        cls.config_path.write_text(yaml.safe_dump(cls.config))
        cls.processor = GoldProcessor(cls.config_path)
        cls.spark = cls.processor.spark
        cls.spark.sparkContext.setLogLevel('ERROR')
        cls.rows = [
            (1, 1, date(2025, 1, 2), 'Open', Decimal('153.40'),
             10, 'Alice', 'BUILDING', 20, 2, Decimal('100.00'),
             Decimal('0.10'), Decimal('0.18'), Decimal('106.20'), 2025, 1),
            (1, 2, date(2025, 1, 2), 'Open', Decimal('153.40'),
             10, 'Alice', 'BUILDING', 21, 3, Decimal('40.00'),
             Decimal('0.00'), Decimal('0.18'), Decimal('47.20'), 2025, 1),
            (2, 1, date(2025, 2, 3), 'Finished', Decimal('118.00'),
             11, 'Bob', 'AUTOMOBILE', 20, 4, Decimal('100.00'),
             Decimal('0.00'), Decimal('0.18'), Decimal('118.00'), 2025, 2),
        ]

    @classmethod
    def tearDownClass(cls):
        cls.processor.close()
        cls.tmp.cleanup()

    def write_source(self, rows, schema=None):
        schema = schema or ', '.join(f'{name} {kind}' for name, kind in
                                     self.config['source']['columns'].items())
        self.spark.createDataFrame(rows, schema).write.format('delta').mode('overwrite')\
            .option('overwriteSchema', 'true').save(self.config['source']['path'])

    def table(self, report, name):
        return self.spark.read.format('delta').load(report['tables'][name]['path'])

    def test_business_contract_and_publication_atomicity(self):
        self.write_source(self.rows)
        report = self.processor.run()
        self.assertEqual(report['source']['version'], 0)
        counts = {'fact_sales': 3, 'fact_orders': 2, 'dim_date': 365,
                  'dim_customer': 2, 'dim_product': 2, 'dim_order_status': 2,
                  'dim_market_segment': 2, 'agg_sales_monthly': 2, 'agg_customer_monthly': 2}
        for name, count in counts.items():
            with self.subTest(table=name):
                actual = self.table(report, name)
                self.assertEqual(actual.count(), count)
                spec = self.config['tables'][name]
                self.assertEqual(actual.select(*spec['primary_key']).distinct().count(), count)
                for column, dtype in spec['columns'].items():
                    self.assertEqual(actual.schema[column].dataType.simpleString(),
                                     {'long': 'bigint', 'integer': 'int'}.get(dtype, dtype))
        sales = self.table(report, 'fact_sales').orderBy('order_id', 'line_number').collect()
        self.assertEqual([r.net_revenue for r in sales],
                         [Decimal('106.20'), Decimal('47.20'), Decimal('118.00')])
        self.assertEqual(sum(r.quantity for r in sales), 9)
        for row in sales:
            self.assertEqual(row.net_revenue, row.revenue_before_tax + row.tax_amount)
        orders = self.table(report, 'fact_orders').orderBy('order_id').collect()
        self.assertEqual(orders[0].line_count, 2)
        self.assertEqual(orders[0].quantity, 5)
        self.assertEqual(orders[0].order_total, Decimal('153.40'))
        self.assertEqual(sum(r.order_total for r in orders), Decimal('271.40'))
        monthly = self.table(report, 'agg_sales_monthly').orderBy('date_key').collect()
        self.assertEqual([r.order_count for r in monthly], [1, 1])
        self.assertEqual([r.date_key for r in monthly], [20250101, 20250201])
        self.assertEqual(sum(r.net_revenue for r in monthly), sum(r.net_revenue for r in sales))
        dates = self.table(report, 'dim_date').orderBy('date').collect()
        self.assertEqual(dates[0].date, date(2025, 1, 1))
        self.assertEqual(dates[-1].date, date(2025, 12, 31))
        self.assertTrue(all((b.date - a.date).days == 1 for a, b in zip(dates, dates[1:])))
        pointer = Path(self.config['publish']['current_manifest'])
        manifest = json.loads(pointer.read_text())
        self.assertEqual(manifest['release_id'], report['release_id'])
        for name in self.config['export']['tables']:
            entry = manifest['tables'][name]
            self.assertTrue(entry['csv_relative_path'])
            self.assertTrue(entry['schema'])
        exported = Path(self.config['export']['root']) / manifest['tables']['agg_sales_monthly']['csv_relative_path']
        csv_rows = []
        for part in exported.glob('part-*.csv'):
            with part.open(encoding='utf-8', newline='') as stream:
                csv_rows.extend(csv.DictReader(stream))
        self.assertEqual(sum(Decimal(r['net_revenue']) for r in csv_rows), Decimal('271.40'))
        self.assertEqual(len(csv_rows), 2)
        again = self.processor.run()
        self.assertNotEqual(again['release_id'], report['release_id'])
        self.assertEqual(self.table(again, 'fact_sales').count(), 3)
        self.assertEqual(self.table(report, 'fact_sales').count(), 3)
        published = pointer.read_bytes()
        half_cent = list(self.rows[0])
        half_cent[4] = Decimal('90.05')
        half_cent[10] = Decimal('100.05')
        half_cent[12] = Decimal('0.00')
        half_cent[13] = Decimal('90.05')
        schema = ', '.join(f'{name} {kind}' for name, kind in self.config['source']['columns'].items())
        half_frames = self.processor.build_tables(self.spark.createDataFrame([half_cent], schema))
        half = half_frames['fact_sales'].first()
        self.assertEqual(half.revenue_before_tax, Decimal('90.05'))
        self.assertEqual(half.discount_amount, Decimal('10.00'))
        self.assertEqual(half.base_price, half.discount_amount + half.revenue_before_tax)
        limits = copy.deepcopy(self.config)
        limits['export']['max_currency_abs'] = '10.00'
        with self.assertRaisesRegex(ValueError, 'Fixed Decimal capacity'):
            GoldExporter(self.spark, limits).publish(half_frames, again)
        self.assertEqual(pointer.read_bytes(), published)
        cases = [
            ('order attributes', 1, 4, Decimal('154.40')),
            ('customer attributes', 1, 6, 'Conflicting name'),
            ('null required', 0, 6, None),
            ('date partition', 0, 15, 2),
            ('financial mismatch', 0, 13, Decimal('106.21')),
        ]
        for label, index, column, value in cases:
            with self.subTest(invalid=label):
                rows = [list(r) for r in self.rows]
                rows[index][column] = value
                self.write_source(rows)
                with self.assertRaises(ValueError):
                    self.processor.run()
                self.assertEqual(pointer.read_bytes(), published)
        self.write_source(self.rows + [self.rows[0]])
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            self.processor.run()
        self.assertEqual(pointer.read_bytes(), published)
        columns = copy.deepcopy(self.config['source']['columns'])
        columns['order_date'] = 'string'
        wrong = [list(r) for r in self.rows]
        for row in wrong:
            row[2] = row[2].isoformat()
        self.write_source(wrong, ', '.join(f'{name} {kind}' for name, kind in columns.items()))
        with self.assertRaisesRegex(ValueError, 'type mismatch'):
            self.processor.run()
        self.assertEqual(pointer.read_bytes(), published)


if __name__ == '__main__':
    unittest.main(verbosity=2)
