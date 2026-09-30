"""命令行入口：跑分块加载样例。"""

import argparse

from .core import Streamer


def build_parser():
    parser = argparse.ArgumentParser(prog="chunk-streaming", description="分块流式加载")
    parser.add_argument("--sample", default="budget", help="budget / lru / dup / scan")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.sample == "budget":
        streamer = Streamer(budget=2)
        for index, chunk in enumerate(["a", "b", "c"]):
            streamer.visit(chunk, index)
        print("loaded=%d evicted=%d" % (len(streamer.loaded), streamer.evicted))
    elif args.sample == "lru":
        streamer = Streamer(budget=2)
        streamer.visit("a", 0)
        streamer.visit("b", 1)
        streamer.visit("a", 2)
        streamer.visit("c", 3)
        print("loaded=%s" % ",".join(streamer.loaded_ids()))
    elif args.sample == "dup":
        streamer = Streamer(budget=2)
        streamer.visit("a", 0)
        streamer.visit("a", 10)
        print("loads=%d" % streamer.loads)
    elif args.sample == "scan":
        streamer = Streamer(budget=100000)
        for index in range(3000):
            streamer.visit("c-%d" % index, index)
        for _ in range(3000):
            streamer.loaded_ids()
        print("scanned=%d" % streamer.scanned_count())
    else:
        raise SystemExit("需要 --sample budget|lru|dup|scan")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
