#!/usr/bin/env python2

import click
import transaction
import multiprocessing as mp

from util.server import server
from benchmarks.models import WikiPage
from wiki import read_docs


@click.command()
@click.option("--batch-size", default=10000, type=click.INT, help="Number of records to add in one transaction", show_default=True)
@click.option("--db-dir", type=click.STRING, default="", help="Directory for the db")
@click.option("--wiki-dir", type=click.STRING, default="wiki_dump", help="Directory with Wikipedia plaintext dump")
def run(batch_size, db_dir, wiki_dir):
    click.echo("Counting number of docs...")

    with server(db_dir=db_dir, compress=True) as db:
        def indexer(docs):
            with transaction.manager:
                for doc in docs:
                    db.add(WikiPage(**doc))
            db.pack()

        it = read_docs(wiki_dir)

        while True:
            docs = []
            last = False
            try:
                for i in range(batch_size):
                    docs.append(next(it))
            except StopIteration:
                last = True
            if docs:
                p = mp.Process(target=indexer, args=(docs,))
                p.start()
                p.join()
            if last:
                break

    click.echo("Stopped the db")


if __name__ == "__main__":
    run()
