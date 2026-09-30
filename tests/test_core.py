import unittest
import random
from collections import OrderedDict

from streaming.core import Streamer


def reference_run(budget, visits):
    """参考实现：预算内 LRU 卸载 + 访问更新 + 幂等。"""
    loaded = OrderedDict()
    loads = 0
    evicted = 0
    for chunk_id, now_ms in visits:
        if chunk_id in loaded:
            loaded[chunk_id] = now_ms
            loaded.move_to_end(chunk_id)
        else:
            loaded[chunk_id] = now_ms
            loads += 1
            while len(loaded) > budget:
                loaded.popitem(last=False)
                evicted += 1
    return set(loaded), loads, evicted


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


class BudgetTest(unittest.TestCase):
    def test_evicts_over_budget(self):
        streamer = Streamer(budget=2)
        for index, chunk in enumerate(["a", "b", "c"]):
            streamer.visit(chunk, index)
        self.assertEqual(len(streamer.loaded), 2)
        self.assertEqual(streamer.evicted, 1)

    def test_never_exceeds_budget(self):
        streamer = Streamer(budget=3)
        for index in range(100):
            streamer.visit("c-%d" % index, index)
            self.assertLessEqual(len(streamer.loaded), 3)
        self.assertEqual(streamer.evicted, 97)

    def test_evicts_least_recently_used(self):
        streamer = Streamer(budget=2)
        streamer.visit("a", 0)
        streamer.visit("b", 1)
        streamer.visit("a", 2)
        streamer.visit("c", 3)
        self.assertEqual(streamer.loaded_ids(), ["a", "c"])
        self.assertEqual(streamer.evicted, 1)


class IdempotentTest(unittest.TestCase):
    def test_revisit_does_not_reload(self):
        streamer = Streamer(budget=2)
        streamer.visit("a", 0)
        streamer.visit("a", 10)
        self.assertEqual(streamer.loads, 1)

    def test_revisit_updates_recency(self):
        streamer = Streamer(budget=2)
        streamer.visit("a", 0)
        streamer.visit("b", 1)
        streamer.visit("a", 2)
        streamer.visit("c", 3)
        self.assertIn("a", streamer.loaded)
        self.assertNotIn("b", streamer.loaded)


class QueryTest(unittest.TestCase):
    def test_loaded_ids_sorted(self):
        streamer = Streamer(budget=4)
        streamer.visit("b", 0)
        streamer.visit("a", 1)
        self.assertEqual(streamer.loaded_ids(), ["a", "b"])

    def test_scanned_starts_at_zero(self):
        self.assertEqual(Streamer(budget=4).scanned_count(), 0)

    def test_scanned_constant_per_query(self):
        streamer = Streamer(budget=100000)
        for index in range(3000):
            streamer.visit("c-%d" % index, index)
        for _ in range(3000):
            streamer.loaded_ids()
        self.assertLessEqual(streamer.scanned_count(), 3000)


class ReferenceComparisonTest(unittest.TestCase):
    def test_matches_reference_on_random_sequences(self):
        rng = random.Random(228)
        for _ in range(200):
            budget = rng.randint(1, 8)
            visits = [
                ("c-%d" % rng.randint(0, 19), rng.randint(0, 1000))
                for _ in range(rng.randint(1, 120))
            ]
            streamer = Streamer(budget=budget)
            for chunk_id, now_ms in visits:
                streamer.visit(chunk_id, now_ms)
            expected_loaded, expected_loads, expected_evicted = reference_run(budget, visits)
            self.assertEqual(set(streamer.loaded), expected_loaded)
            self.assertEqual(streamer.loads, expected_loads)
            self.assertEqual(streamer.evicted, expected_evicted)

    def test_deterministic_replay(self):
        visits = [("c-%d" % (i % 7), i) for i in range(50)]
        results = []
        for _ in range(2):
            streamer = Streamer(budget=3)
            for chunk_id, now_ms in visits:
                streamer.visit(chunk_id, now_ms)
            results.append((streamer.loaded_ids(), streamer.loads, streamer.evicted))
        self.assertEqual(results[0], results[1])


if __name__ == "__main__":
    unittest.main()
