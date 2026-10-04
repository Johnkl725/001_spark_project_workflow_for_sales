"""Regression checks for time windows and honest hourly comparisons.
Run in dashboard container: python -m unittest discover -s /tmp/fx-tests
"""
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dashboard"))
sys.path.insert(0, "/app/dashboard")
from app import LIMA, RANGES, hourly_change, select_range, price_chart, normalize_gold


class ChartTests(unittest.TestCase):
    def setUp(self):
        self.now = pd.Timestamp("2026-09-29 10:00", tz=LIMA)

    def frame(self, minutes, prices):
        return pd.DataFrame({"timestamp": [self.now - pd.Timedelta(minutes=m) for m in minutes], "price": prices})

    def test_hourly_change_uses_boundary_and_rejects_stale_data(self):
        self.assertAlmostEqual(hourly_change(self.frame([60, 1], [3.4, 3.434]), self.now), 1.0)
        self.assertLess(hourly_change(self.frame([60, 1], [3.4, 3.3]), self.now), 0)
        self.assertEqual(hourly_change(self.frame([60, 1], [3.4, 3.4]), self.now), 0)
        for minutes in ([30, 1], [70, 1], [60, 10], [60, -1]):
            self.assertIsNone(hourly_change(self.frame(minutes, [3.4, 3.5]), self.now))

    def test_range_filters_by_clock_and_lima_midnight(self):
        f = self.frame([601, 600, 60, 15, 1, -1], [3.4] * 6)
        self.assertEqual(len(select_range(f, RANGES[0], self.now)[0]), 2)
        self.assertEqual(len(select_range(f, RANGES[1], self.now)[0]), 3)
        today, start = select_range(f, RANGES[2], self.now)
        self.assertEqual(len(today), 4)
        self.assertEqual(start.hour, 0)

    def test_flat_and_single_observation_have_nonzero_y_domain(self):
        for count in (1, 2):
            f = self.frame([2, 1][:count], [3.435] * count)
            spec = price_chart(f, self.now - pd.Timedelta(hours=1), self.now).to_dict()
            low, high = spec['layer'][0]['encoding']['y']['scale']['domain']
            self.assertLess(low, 3.435)
            self.assertGreater(high, 3.435)

    def test_gold_normalization_excludes_other_pairs_and_invalid_prices(self):
        f = pd.DataFrame({"window_start": ["2026-09-29T05:00:00Z"] * 4,
                          "currency_pair": ["USD/PEN", "EUR/PEN", "USD/PEN", "USD/PEN"],
                          "avg_price": [3.4, 4.0, float('inf'), -1],
                          "avg_buy": [None] * 4, "avg_sell": [None] * 4})
        result = normalize_gold(f)
        self.assertEqual(len(result), 3)
        self.assertEqual(result['price'].notna().sum(), 1)
        self.assertEqual(result.iloc[0]['timestamp'].hour, 0)


if __name__ == '__main__':
    unittest.main()
