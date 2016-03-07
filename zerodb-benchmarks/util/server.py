import zerodb

from contextlib import contextmanager
from os import path
from multiprocessing import Process
from zerodb.storage import ZEOServer
from zerodb.transform.compress_zlib import zlib_compressor
from time import sleep

from zerodb.permissions import elliptic
elliptic.register_auth()

DB_DIR = path.dirname(path.dirname(__file__))


class DBCompress(zerodb.DB):
    compressor = zlib_compressor


@contextmanager
def server(
        db_dir=None,
        sock=("localhost", 8001),
        username="test",
        passphrase="testpassword",
        start_server=True,
        debug=False,
        compress=False):

    if compress:
        dbclass = DBCompress
    else:
        dbclass = zerodb.DB

    if not db_dir:
        db_dir = DB_DIR
    conf_path = path.join(db_dir, "conf", "server.zcml")

    if start_server:
        server = Process(target=ZEOServer.run, kwargs={"args": ("-C", conf_path)})
        server.start()
        sleep(0.2)  # Waiting until server starts (no big deal if it didn't yet though)
    zdb = dbclass(sock, username=username, password=passphrase, debug=debug)

    yield zdb

    zdb.disconnect()
    if start_server:
        server.terminate()
        server.join()
