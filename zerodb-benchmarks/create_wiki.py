#!/usr/bin/env python2

import click
import transaction

from util.server import server
from benchmarks.models import WikiPage
from wiki import read_docs


@click.command()
@click.option("--batch-size", default=50, type=click.INT, help="Number of records to add in one transaction", show_default=True)
@click.option("--db-dir", type=click.STRING, default="", help="Directory for the db")
@click.option("--wiki-dir", type=click.STRING, default="wiki_dump", help="Directory with Wikipedia plaintext dump")
def run(batch_size, db_dir, wiki_dir):
    click.echo("Counting number of docs...")
    bctr = 0

    with server(db_dir=db_dir, compress=True) as db:
        for doc in read_docs(wiki_dir):
            page = WikiPage(**doc)
            db.add(page)

            bctr += 1
            if bctr >= batch_size:
                transaction.commit()
                bctr = 0
                db.pack()

        transaction.commit()
        db.pack()

    click.echo("Stopped the db")


if __name__ == "__main__":
    run()
