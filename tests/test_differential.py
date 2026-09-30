"""随机差分测试：实现与参考口径（预算内 LRU 卸载 + 访问更新 + 幂等）逐状态比对。"""

import random
import unittest

from streaming.core import Streamer


class ReferenceStreamer:
    """朴素参考实现：字典存最近使用时刻，超预算时卸载时刻最旧的分块。"""

    def __init__(self, budget):
        self.budget = budget
        self.loaded = {}
        self.loads = 0
        self.evicted = 0

    def visit(self, chunk_id, now_ms):
        if chunk_id in self.loaded:
            self.loaded[chunk_id] = now_ms
            return
        self.loaded[chunk_id] = now_ms
        self.loads += 1
        while len(self.loaded) > self.budget:
            victim = min(self.loaded, key=self.loaded.get)
            del self.loaded[victim]
            self.evicted += 1

    def loaded_ids(self):
        return sorted(self.loaded)


class DifferentialTest(unittest.TestCase):
    def test_200_random_sequences_match_reference(self):
        rng = random.Random(20261001)
        for case_index in range(200):
            budget = rng.randrange(0, 9)
            pool_size = rng.randrange(1, 12)
            length = rng.randrange(0, 80)
            chunks = ["c%d" % i for i in range(pool_size)]
            weights = [rng.randrange(1, 5) for _ in chunks]

            actual = Streamer(budget=budget)
            reference = ReferenceStreamer(budget=budget)
            for step in range(length):
                chunk_id = rng.choices(chunks, weights=weights, k=1)[0]
                now_ms = step
                actual.visit(chunk_id, now_ms)
                reference.visit(chunk_id, now_ms)
                self.assertEqual(
                    actual.loaded_ids(),
                    reference.loaded_ids(),
                    msg="case=%d step=%d budget=%d" % (case_index, step, budget),
                )
                self.assertEqual(actual.loads, reference.loads)
                self.assertEqual(actual.evicted, reference.evicted)


if __name__ == "__main__":
    unittest.main()
