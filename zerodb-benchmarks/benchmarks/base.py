class Benchmark(object):
    def __init__(self, db, limit=20, prefetch=True):
        self.db = db
        self.limit = limit
        self.prefetch = prefetch

    def _run_query(self, q):
        # Subclass must set self.model
        self.db[self.model].query(q, limit=self.limit, prefetch=self.prefetch)
