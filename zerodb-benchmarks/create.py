#!/usr/bin/env python2

import click
import transaction
import random
import multiprocessing as mp

from util.server import server
from benchmarks.models import TestRecord
from util.markov import MarkovChain

pool = None
textgen = None


def generate_text(size):
    return textgen.generate_block(size)


@click.command()
@click.option("--batch-size", default=20000, type=click.INT, help="Number of records to add in one transaction", show_default=True)
@click.option("--num-batches", default=5, type=click.INT, help="Number of transactions for initial data creation", show_default=True)
@click.option("--text-size", default=1000, type=click.INT, help="Size of the generated text blob (text search benchmark)", show_default=True)
@click.option("--min-int", default=0, type=click.INT, help="Minimal integer value (for range queries)", show_default=True)
@click.option("--max-int", default=1000000, type=click.INT, help="Maximum integer value (for range queries)", show_default=True)
@click.option("--min-float", default=0, type=click.FLOAT, help="Minimal float value (for range queries)", show_default=True)
@click.option("--max-float", default=1000000, type=click.FLOAT, help="Maximum float value (for range queries)", show_default=True)
@click.option("--use-multiprocessing", is_flag=True, help="Use multiprocessing for random text generation (Markov chains)")
def run(batch_size, num_batches, text_size, min_int, max_int, min_float, max_float, use_multiprocessing):
    global textgen
    global pool

    click.echo("Creating Markov chains generator...")
    textgen = MarkovChain()
    markov_cache = []
    pool = mp.Pool(mp.cpu_count())

    click.echo("Starting database...")
    with server() as db:
        with click.progressbar(range(num_batches * batch_size), label="Creating test records") as bar:
            for i_batch in bar:

                if use_multiprocessing:
                    if not markov_cache:
                        markov_cache = pool.map(generate_text, [text_size] * mp.cpu_count() * 5)
                    text = markov_cache.pop()

                else:
                    text = textgen.generate_block(text_size)

                db.add(TestRecord(
                    text=text,
                    int_val=random.randrange(min_int, max_int),
                    float_val=(random.random() * (max_float - min_float) + min_float)))
                if i_batch % batch_size == 0:
                    transaction.commit()

            transaction.commit()
            click.echo("Stopping the db...")

    click.echo("Stopped the db")

if __name__ == "__main__":
    run()
