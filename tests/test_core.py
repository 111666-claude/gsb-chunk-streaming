import unittest

from streaming.core import Streamer


class VisitTest(unittest.TestCase):
    def test_visit_loads_chunk(self):
        streamer = Streamer(budget=4)
        streamer.visit("a", 0)
        self.assertEqual(streamer.loaded_ids(), ["a"])

    def test_visit_under_budget_keeps_all(self):
        streamer = Streamer(budget=4)
        streamer.visit("a", 0)
        streamer.visit("b", 1)
        self.assertEqual(len(streamer.loaded), 2)


class QueryTest(unittest.TestCase):
    def test_loaded_ids_sorted(self):
        streamer = Streamer(budget=4)
        streamer.visit("b", 0)
        streamer.visit("a", 1)
        self.assertEqual(streamer.loaded_ids(), ["a", "b"])

    def test_scanned_starts_at_zero(self):
        self.assertEqual(Streamer(budget=4).scanned_count(), 0)


if __name__ == "__main__":
    unittest.main()
