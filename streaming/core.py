"""地形分块流式加载：按视点加载邻近分块、内存预算内按最近使用卸载、重复加载幂等。

口径（见 README）：
- 已加载分块数不超过 ``budget``；超出时卸载最近使用时刻最旧的分块。
- ``visit`` 会更新该分块的最近使用时刻（LRU 依据）。
- 重复 ``visit`` 已加载分块不增加 ``loads``，只更新访问时刻。
- ``loaded_ids`` 不再每次全扫已加载表，排序结果惰性缓存，
  仅在加载集合发生变化后的首次查询重建。
"""

from collections import OrderedDict

CHUNK = 64.0
BUDGET = 4


class Streamer:
    """分块流式加载器。

    已加载分块保存在 ``OrderedDict`` 中，键的顺序即 LRU 顺序（最近使用
    的在队尾）。单次 ``visit`` 只做哈希查找、移动队尾以及至多一次弹出
    队首，摊还 O(1)；除当前持有的分块外不保留任何历史分块，内存 O(budget)。
    """

    def __init__(self, budget=BUDGET):
        self.budget = budget
        self.loaded = OrderedDict()
        self.loads = 0
        self.evicted = 0
        self.scanned = 0
        self._sorted_ids = []

    def visit(self, chunk_id, now_ms):
        """访问一个分块。

        已加载则只刷新最近使用时刻（移到 LRU 队尾），不增加 ``loads``；
        未加载则装入，装入后若超出 ``budget``，弹出最近使用时刻最旧的
        队首分块并把 ``evicted`` 加一。返回该分块是否为新加载。
        """
        if chunk_id in self.loaded:
            self.loaded[chunk_id] = now_ms
            self.loaded.move_to_end(chunk_id)
            return False
        self.loaded[chunk_id] = now_ms
        self.loads += 1
        self._sorted_ids = None
        while len(self.loaded) > self.budget:
            self.loaded.popitem(last=False)
            self.evicted += 1
        return True

    def loaded_ids(self):
        """已加载分块的有序 id 列表。

        排序结果惰性缓存：只有加载集合在上次查询后发生变化时才重建一次
        （此时扫描的表项计入 ``scanned``），之后重复查询直接返回缓存副本。
        """
        if self._sorted_ids is None:
            self.scanned += len(self.loaded)
            self._sorted_ids = sorted(self.loaded)
        return list(self._sorted_ids)

    def scanned_count(self):
        return self.scanned
