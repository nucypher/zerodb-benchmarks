import models
import random
import time

from zerodb.query import Contains


class Benchmark(object):
    def __init__(self, db, limit=20, prefetch=True):
        self.db = db
        self.limit = limit
        self.prefetch = prefetch


class TextBenchmark(Benchmark):
    def __init__(self, db, min_words=1, max_words=4, limit=20, prefetch=True):
        super(TextBenchmark, self).__init__(db, limit=limit, prefetch=prefetch)
        self.words = filter(lambda x: len(x) >= 4,
                db._root['catalog__testrecord']['text'].lexicon._wids.keys())
        self.min_words = min_words
        self.max_words = max_words

    def _choose_words(self):
        n = random.randrange(self.min_words, self.max_words + 1)
        return random.sample(self.words, n)

    def _run_query(self, q):
        self.db[models.TestRecord].query(q, limit=self.limit, prefetch=self.prefetch)

    def read_one(self):
        self._run_query(Contains("text", random.choice(self.words)))

    def read_star(self):
        self._run_query(Contains("text", random.choice(self.words)[:2] + "*"))

    def read_many(self):
        self._run_query(Contains("text", " ".join(self._choose_words())))

    def read_many_star(self):
        words = self._choose_words()
        words = [words[0][:2] + "*"] + words[1:]
        self._run_query(Contains("text", " ".join(words)))
