import bz2
import pymarkovchain
import random
import os.path

try:
    import cPickle as pickle
except ImportError:
    import pickle


class MarkovChain(pymarkovchain.MarkovChain):
    def __init__(self, path=None):
        if not path:
            path = os.path.join(os.path.dirname(__file__), "data", "reuters.mc.bz2")
        f = bz2.BZ2File(path)
        self.db = pickle.load(f)
        f.close()

    def generate_block(self, size=1000):
        """Generate random block of text with mean length of size"""

        out = ""
        while len(out) < size:
            new = self.generateString()
            if len(out) + len(new) + 1 > size:
                break
            if out == "":
                out = new
            else:
                out += "\n" + new
        if random.random() < 0.5:
            out += "\n" + new
        return out
