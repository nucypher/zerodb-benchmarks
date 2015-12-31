#!/usr/bin/env python2

import click
import time
import sys
import pandas
import zerodb

import benchmarks

from os import path
from datetime import datetime

SOCKET = ("localhost", 8001)
username = "test"
password = "testpassword"
DATE_FMT = "%Y-%m-%d-%H-%M"


def run_benchmark(cls, methodname, n_subcycles=5, n_cycles=1000, output_dir="out", **kw):
    fname = path.join("out", "{0}.{1}.{2}.csv".format(cls.__name__, methodname, datetime.utcnow().strftime(DATE_FMT)))
    db = zerodb.DB(SOCKET, username=username, password=password)
    bench = cls(db, **kw)
    method = getattr(bench, methodname)
    out = []
    for i in range(n_cycles):
        t0 = time.time()
        for j in range(n_subcycles):
            method()
        dt = time.time() - t0
        d_out = {
            "size": db._connection._cache.total_estimated_size,
            "ngc": db._connection._cache.cache_non_ghost_count,
            "query_t": float(dt) / n_subcycles,
            "n_queries": (i + 1) * n_subcycles}
        print i, d_out
        out.append(d_out)
        df = pandas.DataFrame(out)
        df.to_csv(fname)
    db.disconnect()
    return out


if __name__ == "__main__":
    classname = sys.argv[1] if len(sys.argv) > 1 else "TextBenchmark"
    methodname = sys.argv[2] if len(sys.argv) > 2 else "read_one"
    cls = getattr(benchmarks, classname)
    run_benchmark(cls, methodname)
