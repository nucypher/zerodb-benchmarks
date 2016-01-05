import random
import models

from zerodb.query import InRange, Gt, Lt, Eq
from base import Benchmark


class NumericBenchmark(Benchmark):
    model = models.TestRecord
    field_name = None

    def __init__(self, db, min=0, max=1000000, limit=20, prefetch=True):
        super(NumericBenchmark, self).__init__(db, limit=limit, prefetch=prefetch)
        self.min = min
        self.max = max
        self.values = list(db._root["catalog__testrecord"][self.field_name]._fwd_index.keys())
        db._connection._cache.full_sweep()

    @property
    def random_value(self):
        return random.choice(self.values)

    def eq(self):
        self._run_query(Eq(self.field_name, self.random_value))

    def lt(self):
        self._run_query(Lt(self.field_name, self.random_value))

    def gt(self):
        self._run_query(Gt(self.field_name, self.random_value))

    def inrange(self):
        self._run_query(InRange(self.field_name, self.random_value, self.random_value))


class FloatBenchmark(NumericBenchmark):
    field_name = "float_val"


class IntBenchmark(NumericBenchmark):
    field_name = "int_val"
