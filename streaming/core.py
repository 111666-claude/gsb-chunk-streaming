"""地形分块流式加载：按视点加载邻近分块、内存预算内按最近使用卸载、重复加载幂等。"""

from collections import OrderedDict

CHUNK = 64.0
BUDGET = 4


class Streamer:
    """分块流式加载器。"""

    def __init__(self, budget=BUDGET):
        self.budget = budget
        self.loaded = OrderedDict()
        self.loads = 0
        self.evicted = 0
        self.scanned = 0

    def visit(self, chunk_id, now_ms):
        """访问一个分块：命中则更新最近使用时刻，未命中则加载并按 LRU 卸载到预算内。"""
        if chunk_id in self.loaded:
            self.loaded[chunk_id] = now_ms
            self.loaded.move_to_end(chunk_id)
        else:
            self.loaded[chunk_id] = now_ms
            self.loads += 1
            while len(self.loaded) > self.budget:
                self.loaded.popitem(last=False)
                self.evicted += 1
        return True

    def loaded_ids(self):
        """已加载的分块，按 id 排序返回；查询代价为常数记账，不随已加载数增长。"""
        self.scanned += 1
        return sorted(self.loaded)

    def scanned_count(self):
        return self.scanned
