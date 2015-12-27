import click
import transaction
import random

from benchmarks.server import server
from benchmarks.models import TestRecord
from benchmarks.markov import MarkovChain


@click.command()
@click.option("--batch-size", default=10, type=click.INT)
@click.option("--num-batches", default=100, type=click.INT)
@click.option("--text-size", default=1000, type=click.INT)
@click.option("--min-int", default=0, type=click.INT)
@click.option("--max-int", default=1000000, type=click.INT)
@click.option("--min-float", default=0, type=click.FLOAT)
@click.option("--max-float", default=1000000, type=click.FLOAT)
def run(batch_size, num_batches, text_size, min_int, max_int, min_float, max_float):
    click.echo("Creating Markov chains generator...")
    textgen = MarkovChain()
    click.echo("Starting database...")
    with server() as db:
        with click.progressbar(length=num_batches, label="Creating test records") as bar:
            for i_batch in xrange(num_batches):
                bar.update(i_batch)

                db.add([TestRecord(
                    text=textgen.generate_block(text_size),
                    int_val=random.randrange(min_int, max_int),
                    float_val=(random.random() * (max_float - min_float) + min_float))
                    for i in xrange(batch_size)])
                transaction.commit()
        click.echo("Stopping the database...")

if __name__ == "__main__":
    run()
