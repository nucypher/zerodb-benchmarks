class Benchmark(object):
    def __init__(self, db, limit=20, prefetch=True):
        self.db = db
        self.limit = limit
        self.prefetch = prefetch
