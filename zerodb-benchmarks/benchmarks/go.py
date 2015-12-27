"""
Benchmarks to do:
  Types:
    * Fulltext search:
      - full keywords (1, <= k)
      - prefixes (1, mixed with <= k keywords)
    * Range queries:
      - <, >, InRange; for ints, floats

With db sizes:
  S_db = 100M, 1G, 10G etc.

With cache sizes:
  S_c = 1M, 10M, 100M, 1G

Over time (with cache filled):
  out: time, #queries, cache filled, performance

Object creation:
  * "fillout" mode
  * benchmark mode

Extra performance tests for:
  * object creation vs # processes
  * selected read queries in parallel

cProfile to find bottlenecks in each case (to point what to rewrite in C, for example).
"""
