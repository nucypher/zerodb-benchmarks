#!/usr/bin/env python2

import time
import sys
import benchmarks
import pandas
import zerodb

SOCKET = ("localhost", 8001)
username = "test"
password = "testpassword"


def run_benchmark(cls, methodname, n_subcycles=50, n_cycles=1000, **kw):
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
    db.disconnect()
    return out


if __name__ == "__main__":
    classname = sys.argv[1] if len(sys.argv) > 1 else "TextBenchmark"
    methodname = sys.argv[2] if len(sys.argv) > 2 else "read_one"
    fname = "{0}.{1}.csv".format(classname, methodname)
    cls = getattr(benchmarks, classname)

    df = pandas.DataFrame(run_benchmark(cls, methodname))
    df.to_csv(fname)
