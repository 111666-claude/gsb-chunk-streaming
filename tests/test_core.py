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


class BudgetTest(unittest.TestCase):
    def test_eviction_keeps_loaded_within_budget(self):
        streamer = Streamer(budget=2)
        streamer.visit("a", 0)
        streamer.visit("b", 1)
        streamer.visit("c", 2)
        self.assertEqual(len(streamer.loaded), 2)
        self.assertEqual(streamer.evicted, 1)
        self.assertEqual(streamer.loaded_ids(), ["b", "c"])

    def test_invariant_holds_on_every_step(self):
        streamer = Streamer(budget=3)
        for index in range(50):
            streamer.visit("c-%d" % index, index)
            self.assertLessEqual(len(streamer.loaded), 3)
        self.assertEqual(streamer.evicted, 47)
        self.assertEqual(streamer.loads, 50)

    def test_zero_budget_evicts_every_load(self):
        streamer = Streamer(budget=0)
        streamer.visit("a", 0)
        streamer.visit("b", 1)
        self.assertEqual(len(streamer.loaded), 0)
        self.assertEqual(streamer.loads, 2)
        self.assertEqual(streamer.evicted, 2)


class LruOrderTest(unittest.TestCase):
    def test_revisit_refreshes_lru_position(self):
        streamer = Streamer(budget=2)
        streamer.visit("a", 0)
        streamer.visit("b", 1)
        streamer.visit("a", 2)
        streamer.visit("c", 3)
        self.assertEqual(streamer.loaded_ids(), ["a", "c"])
        self.assertEqual(streamer.evicted, 1)

    def test_eviction_order_tracks_oldest_use_time(self):
        streamer = Streamer(budget=3)
        streamer.visit("a", 0)
        streamer.visit("b", 1)
        streamer.visit("c", 2)
        streamer.visit("a", 10)
        streamer.visit("b", 11)
        streamer.visit("d", 12)
        self.assertEqual(streamer.loaded_ids(), ["a", "b", "d"])


class IdempotentLoadTest(unittest.TestCase):
    def test_revisit_does_not_increase_loads(self):
        streamer = Streamer(budget=2)
        streamer.visit("a", 0)
        self.assertFalse(streamer.visit("a", 10))
        self.assertEqual(streamer.loads, 1)
        self.assertEqual(streamer.evicted, 0)

    def test_revisit_updates_use_time_and_stays_loaded(self):
        streamer = Streamer(budget=2)
        streamer.visit("a", 0)
        streamer.visit("b", 1)
        streamer.visit("a", 2)
        self.assertEqual(streamer.loaded_ids(), ["a", "b"])
        self.assertEqual(streamer.loads, 2)


class QueryCostTest(unittest.TestCase):
    def test_repeated_queries_do_not_full_scan(self):
        streamer = Streamer(budget=100000)
        for index in range(3000):
            streamer.visit("c-%d" % index, index)
        for _ in range(3000):
            streamer.loaded_ids()
        self.assertLessEqual(streamer.scanned_count(), 3000)

    def test_scanned_grows_once_per_loaded_set_change(self):
        streamer = Streamer(budget=10)
        streamer.visit("a", 0)
        streamer.loaded_ids()
        streamer.loaded_ids()
        self.assertEqual(streamer.scanned_count(), 1)
        streamer.visit("a", 5)
        streamer.loaded_ids()
        self.assertEqual(streamer.scanned_count(), 1)
        streamer.visit("b", 6)
        streamer.loaded_ids()
        self.assertEqual(streamer.scanned_count(), 3)


class DeterminismTest(unittest.TestCase):
    def test_same_sequence_same_result(self):
        sequence = [("a", 0), ("b", 1), ("a", 2), ("c", 3), ("b", 4), ("d", 5)]

        def run():
            streamer = Streamer(budget=2)
            states = []
            for chunk_id, now_ms in sequence:
                streamer.visit(chunk_id, now_ms)
                states.append((streamer.loaded_ids(), streamer.loads, streamer.evicted))
            return states

        self.assertEqual(run(), run())


if __name__ == "__main__":
    unittest.main()
