import zerodb

from contextlib import contextmanager
from os import path
from multiprocessing import Process
from zerodb.storage import ZEOServer
from time import sleep

from zerodb.permissions import elliptic
elliptic.register_auth()

DEFAULT_CONF_PATH = path.join(path.dirname(path.dirname(__file__)), "conf", "server.zcml")


@contextmanager
def server(conf_path=DEFAULT_CONF_PATH,
        sock=("localhost", 8001),
        username="test",
        passphrase="testpassword",
        start_server=True,
        debug=False):

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
