"""地形分块流式加载：按视点加载邻近分块、内存预算内按最近使用卸载、重复加载幂等。

缺陷：加载不看内存预算（只增不减）、没有卸载、最近使用时刻不更新（LRU 失效）、查询全扫已加载表。
"""

CHUNK = 64.0
BUDGET = 4


class Streamer:
    """分块流式加载器。"""

    def __init__(self, budget=BUDGET):
        self.budget = budget
        self.loaded = {}
        self.loads = 0
        self.evicted = 0
        self.scanned = 0

    def visit(self, chunk_id, now_ms):
        """访问一个分块。缺陷：不按预算卸载、重复访问也重复计加载次数。"""
        self.loaded[chunk_id] = now_ms
        self.loads += 1
        return True

    def loaded_ids(self):
        """已加载的分块。缺陷：每次全扫已加载表。"""
        self.scanned += len(self.loaded)
        return sorted(self.loaded)

    def scanned_count(self):
        return self.scanned
