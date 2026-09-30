# chunk-streaming

地形分块流式加载：按视点加载邻近分块、内存预算内按最近使用卸载、重复加载幂等，只用 Python 标准库。

```
python3 -m streaming.cli --sample budget
python3 -m streaming.cli --sample lru
python3 -m streaming.cli --sample dup
python3 -m unittest discover -s tests -v
```

## 口径（README 为准）

- **内存预算**：已加载分块数不超过 `budget`；超出的按**最近使用时刻最旧**的卸载，`evicted` 加一。
- **访问即更新**：`visit` 必须更新该分块的最近使用时刻（LRU 依据）。
- **加载幂等**：重复 `visit` 已加载的分块不增加 `loads`，只更新访问时刻。
- **查询代价**：`loaded_ids` 不许每次全扫已加载表，`scanned_count` 不随已加载数线性增长。
- **规模**：单场景 20 万个分块、每秒 10 万次访问与查询，单次摊还 O(1)，内存 O(budget)。

## 输出契约（不改格式）

```
loaded=2 evicted=1
loaded=a,c
loads=1
scanned<=3000
```

## 目录

```
streaming/core.py   预算、LRU 与幂等
streaming/cli.py    命令行入口
tests/              unittest 用例
```
