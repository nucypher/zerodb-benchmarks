import zerodb

from contextlib import contextmanager
from os import path
from multiprocessing import Process
from zerodb.storage import ZEOServer
from time import sleep

from zerodb.permissions import elliptic
elliptic.register_auth()

DB_DIR = path.dirname(path.dirname(__file__))


@contextmanager
def server(db_dir=None,
        sock=("localhost", 8001),
        username="test",
        passphrase="testpassword",
        start_server=True,
        debug=False):

    if not db_dir:
        db_dir = DB_DIR
    conf_path = path.join(db_dir, "conf", "server.zcml")

    if start_server:
        server = Process(target=ZEOServer.run, kwargs={"args": ("-C", conf_path)})
        server.start()
        sleep(0.2)  # Waiting until server starts (no big deal if it didn't yet though)
    zdb = zerodb.DB(sock, username=username, password=passphrase, debug=debug)

    yield zdb

    zdb.disconnect()
    if start_server:
        server.terminate()
        server.join()
