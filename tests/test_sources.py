"""BCRP sample is from a live response; SBS HTML is a synthetic contract fixture."""
import unittest
import pandas as pd

from ingestion.sources import parse_bcrp, parse_market, parse_sbs

BCRP = {
    "config": {"series": [
        {"name": "Tipo de cambio - TC Interbancario (S/ por US$) - Compra"},
        {"name": "Tipo de cambio - TC Interbancario (S/ por US$) - Venta"},
    ]},
    "periods": [
        {"name": "25.Set.26", "values": ["3.41642857142857", "3.41992857142857"]},
        {"name": "28.Set.26", "values": ["n.d.", "n.d."]},
    ],
}
SBS = '''<span id="ctl00_lblFecha">25/09/2026</span>
<table><tr><th>Moneda</th><th>Venta (S/)</th><th>Compra (S/)</th></tr>
<tr><td>Dólar de N.A.</td><td>3,420</td><td>3,410</td></tr>
<tr><td>Dólar Canadiense</td><td>2.5</td><td>2.4</td></tr></table>'''


class SourceTests(unittest.TestCase):
    def test_bcrp_skips_unpublished_and_preserves_source_date(self):
        rows = parse_bcrp(BCRP)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["timestamp"], "2026-09-25T05:00:00+00:00")
        self.assertAlmostEqual(rows[0]["buy"], 3.41642857142857)

    def test_bcrp_rejects_swapped_series(self):
        with self.assertRaises(ValueError):
            parse_bcrp(dict(BCRP, config={"series": list(reversed(BCRP["config"]["series"]))}))

    def test_sbs_uses_headers_and_usd_only(self):
        row = parse_sbs(SBS)[0]
        self.assertEqual((row["buy"], row["sell"]), (3.41, 3.42))

    def test_sbs_refuses_missing_or_ambiguous_date(self):
        for html in (SBS.replace("25/09/2026", ""), SBS + '<span id="lblFecha2">24/09/2026</span>'):
            with self.assertRaises(ValueError):
                parse_sbs(html)

    def test_sbs_refuses_block_page(self):
        with self.assertRaises(ValueError):
            parse_sbs("Request unsuccessful. Incapsula incident ID: 1234")

    def test_market_preserves_real_time_and_null_bid_ask(self):
        frame = pd.DataFrame({"Close": [3.4]}, index=pd.to_datetime(["2026-09-25T15:00:00Z"]))
        row = parse_market(frame)[0]
        self.assertEqual(row["timestamp"], "2026-09-25T15:00:00+00:00")
        self.assertEqual(row["price"], 3.4)
        self.assertIsNone(row["buy"])
        self.assertIsNone(row["sell"])

    def test_market_rejects_empty_and_naive(self):
        for frame in (pd.DataFrame(), pd.DataFrame({"Close": [3.4]}, index=pd.to_datetime(["2026-09-25"]))):
            with self.assertRaises(ValueError):
                parse_market(frame)


if __name__ == "__main__":
    unittest.main()
