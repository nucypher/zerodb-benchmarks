#!/usr/bin/env python2

import bprofile
import click
import csv
import time
import sys
import pstats
import zerodb

import benchmarks

from os import path
from datetime import datetime

SOCKET = ("localhost", 8001)
username = "test"
password = "testpassword"
DATE_FMT = "%Y-%m-%d-%H-%M"
PROGRESS_FMT = "[{percent:.2%}] query time: {query_t:.4f}, cache filled: {size:.2f} MB, {n_queries} queries"


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

    with open(fname, "wb") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["n_queries", "size", "ngc", "query_t"])

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
            size = db._connection._cache.total_estimated_size
            ngc = db._connection._cache.cache_non_ghost_count
            query_t = float(dt) / n_subcycles
            n_queries = (i + 1) * n_subcycles

            click.echo(PROGRESS_FMT.format(size=size / 1e6, query_t=query_t, n_queries=n_queries, percent=float(i) / n_cycles))
            csv_writer.writerow(map(str, [n_queries, size, ngc, query_t]))

    db.disconnect()


@click.command()
@click.option("--test-name", nargs=2, type=click.Tuple([unicode, unicode]), default=("TextBenchmark", "read_one"), help="Which test to run", show_default=True)
@click.option("--with-profiling", is_flag=True, help="Profiling with bprofile (cProfile + call graph in png)")
@click.option("--n-cycles", type=click.INT, default=1000, help="Number of external cycles (= how many times we save stats)", show_default=True)
@click.option("--n-subcycles", type=click.INT, default=5, help="Number of internal cycles (stats are saved after an internal loop is complete)", show_default=True)
@click.option("--output-dir", type=click.STRING, default="out", show_default=True)
def run(test_name, with_profiling, n_cycles, n_subcycles, output_dir):
    classname, methodname = test_name
    cls = getattr(benchmarks, classname)
    click.echo("Running benchmark for %s.%s" % (classname, methodname))
    run_benchmark(cls, methodname, n_cycles=n_cycles, n_subcycles=n_subcycles, output_dir=output_dir, with_profiling=with_profiling)

if __name__ == "__main__":
    run()
