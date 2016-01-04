#!/usr/bin/env python2

import bprofile
import click
import time
import sys
import pandas
import pstats
import zerodb

import benchmarks

from os import path
from datetime import datetime

SOCKET = ("localhost", 8001)
username = "test"
password = "testpassword"
DATE_FMT = "%Y-%m-%d-%H-%M"


def run_benchmark(cls, methodname, n_subcycles=5, n_cycles=1000, output_dir="out", with_profiling=False, **kw):
    fname_base = "{0}.{1}.{2}.".format(cls.__name__, methodname, datetime.utcnow().strftime(DATE_FMT))
    fname_base = path.join(output_dir, fname_base)
    fname = fname_base + "csv"
    png_name = fname_base + "png"
    stats_name = fname_base + "pstats"
    click.echo("Saving to files:")
    click.echo("  Benchmark results: %s" % fname)
    click.echo("  Profiling graph: %s" % png_name)
    click.echo("  Profiling stats: %s" % stats_name)
    click.echo("=============================")

    if with_profiling:
        profiler = bprofile.BProfile(png_name)

    db = zerodb.DB(SOCKET, username=username, password=password)
    bench = cls(db, **kw)
    method = getattr(bench, methodname)
    out = []
    for i in range(n_cycles):
        if with_profiling:
            profiler.start()
        t0 = time.time()
        for j in range(n_subcycles):
            method()
        dt = time.time() - t0
        if with_profiling:
            profiler.stop()
            pstats.Stats(profiler.profiler).dump_stats(stats_name)
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
@click.option("--with-profiling", is_flag=True)
@click.option("--n-cycles", type=click.INT, default=1000)
@click.option("--n-subcycles", type=click.INT, default=5)
@click.option("--output-dir", type=click.STRING, default="out")
def run(test_name, with_profiling, n_cycles, n_subcycles, output_dir):
    classname, methodname = test_name
    cls = getattr(benchmarks, classname)
    click.echo("Running benchmark for %s.%s" % (classname, methodname))
    run_benchmark(cls, methodname, n_cycles=n_cycles, n_subcycles=n_subcycles, output_dir=output_dir, with_profiling=with_profiling)

if __name__ == "__main__":
    run()
