import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from ingestion.producer import Outbox, Publisher, make_event


def observation(**changes):
    return dict({
        "timestamp": "2026-09-25T05:00:00+00:00", "source": "SBS",
        "currency_pair": "USD/PEN", "buy": 3.7, "sell": 3.8, "price": None,
        "rate_type": "official_daily", "time_precision": "date",
        "source_url": "https://www.sbs.gob.pe/",
    }, **changes)


class EventTests(unittest.TestCase):
    def test_identity_ignores_poll_time_but_preserves_corrections(self):
        original = make_event(observation())
        self.assertEqual(original["event_id"], make_event(observation())["event_id"])
        self.assertNotEqual(original["event_id"], make_event(observation(sell=3.81))["event_id"])

    def test_no_invalid_prices_or_naive_time(self):
        for changes in ({"buy": float("nan")}, {"sell": -1}, {"timestamp": "2026-09-25"}):
            with self.assertRaises(ValueError):
                make_event(observation(**changes))

    def test_market_does_not_require_artificial_bid_ask(self):
        event = make_event(observation(buy=None, sell=None, price=3.75))
        self.assertIsNone(event["buy"])
        self.assertEqual(json.loads(json.dumps(event))["price"], 3.75)


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "state.sqlite3"
        self.outbox = Outbox(self.path)
        self.event = make_event(observation())

    def tearDown(self):
        self.outbox.close()
        self.directory.cleanup()

    def test_pending_survives_restart_and_duplicate_insert(self):
        self.assertTrue(self.outbox.add(self.event))
        self.assertFalse(self.outbox.add(self.event))
        self.outbox.close()
        self.outbox = Outbox(self.path)
        self.assertEqual(len(self.outbox.pending()), 1)

    def test_only_ack_marks_delivered_and_failure_retries(self):
        self.outbox.add(self.event)
        client = Mock()
        publisher = Publisher(client, self.outbox)
        publisher.pump()
        callback = client.produce.call_args.kwargs["on_delivery"]
        self.assertEqual(len(self.outbox.pending()), 1)
        publisher.pump()
        self.assertEqual(client.produce.call_count, 1)
        callback("timeout", None)
        self.assertEqual(len(self.outbox.pending()), 1)
        publisher.retry_at.clear()
        publisher.pump()
        self.assertEqual(client.produce.call_count, 2)
        client.produce.call_args.kwargs["on_delivery"](None, Mock())
        self.assertEqual(self.outbox.pending(), [])
        self.assertFalse(self.outbox.add(self.event))

    def test_buffer_full_keeps_event_retryable(self):
        self.outbox.add(self.event)
        publisher = Publisher(Mock(produce=Mock(side_effect=BufferError)), self.outbox)
        publisher.pump()
        self.assertFalse(publisher.inflight)
        self.assertEqual(len(self.outbox.pending()), 1)


if __name__ == "__main__":
    unittest.main()
