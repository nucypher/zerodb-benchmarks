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
    click.echo("Saving to file %s" % fname)
    click.echo("=============================")
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


@click.command()
@click.option("--test-name", nargs=2, type=click.Tuple([unicode, unicode]), default=("TextBenchmark", "read_one"))
def run(test_name):
    classname, methodname = test_name
    cls = getattr(benchmarks, classname)
    click.echo("Running benchmark for %s.%s" % (classname, methodname))
    run_benchmark(cls, methodname)

if __name__ == "__main__":
    run()
