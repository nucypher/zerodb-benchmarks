#!/usr/bin/env python2

import click
import path
import transaction
import psutil
import os
import gc
import multiprocessing as mp

from zerodb.models import Model, Field

from util.server import server
from benchmarks.models import WikiPage
from wiki import read_doc


COMMIT_THRESHOLD = 55.0
TERM_THRESHOLD = 30.0


class PageName(Model):
    processed = Field(default=False)
    fname = Field()


@click.command()
@click.option("--db-dir", type=click.STRING, default="", help="Directory for the db")
@click.option("--wiki-dir", type=click.STRING, default="wiki_dump", help="Directory with Wikipedia plaintext dump")
def run(db_dir, wiki_dir):
    with server(db_dir=db_dir, compress=True) as db:

        def indexer():
            process = psutil.Process(os.getpid())
            names_to_process = list(db[PageName].query(processed=False))
            total_names = len(db[PageName])

            for i, p in enumerate(names_to_process):
                print(1.0 - (float(len(names_to_process) - i) / total_names))
                for doc in read_doc(p.fname):
                    db.add(WikiPage(**doc))
                pname = p
                pname.processed = True
                if process.memory_percent() > COMMIT_THRESHOLD:
                    transaction.commit()
                    gc.collect()
                    if process.memory_percent() > TERM_THRESHOLD:
                        db.pack()
                        return

            print "All done"
            transaction.commit()
            db.pack()

        ctr = 0
        for fname in map(str, path.path(wiki_dir).walkfiles("*.bz2")):
            if not db[PageName].query(fname=fname):
                db[PageName].add(PageName(fname=fname))
                ctr += 1
        transaction.commit()
        print("Added {0} files, total: {1}".format(ctr, len(db[PageName])))

        while db[PageName].query(processed=False, limit=1):
            p = mp.Process(target=indexer)
            p.start()
            p.join()
            transaction.abort()  # Need beecause of MVCC

    click.echo("Stopped the db")


if __name__ == "__main__":
    run()
