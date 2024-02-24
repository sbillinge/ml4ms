"""Helps manage mongodb setup and connections."""
import os
from contextlib import contextmanager

from ml4ms.chained_db import ChainDB
from ml4ms.fsclient import FileSystemClient


def load_local_database(db, client, rc):
    """Loads a local database"""
    # make sure that we expand user stuff
    db["url"] = os.path.expanduser(db["url"])
    # import all of the data
    client.load_database(db)


def load_mongo_database(db, client):
    """Load a mongo database."""
    client.load_database(db)


def load_database(db, client, rc):
    """Loads a database"""
    if rc.client in ("mongo", "mongodb"):
        load_mongo_database(db, client)
        return
    url = db["url"]
    if os.path.exists(os.path.expanduser(url)):
        load_local_database(db, client, rc)
    else:
        raise ValueError("Do not know how to load this kind of database: " "{}".format(db))


def dump_local_database(db, client, rc):
    """Dumps a local database"""
    # dump all of the data
    client.dump_database(db)
    return


def dump_database(db, client, rc):
    """Dumps a database"""
    # do not dump mongo db
    if rc.client in ("mongo", "mongodb"):
        return
    url = db["url"]
    if os.path.exists(url):
        dump_local_database(db, client, rc)
    else:
        raise ValueError("Do not know how to dump this kind of database")


def open_dbs(rc, colls=None):
    """Open the databases

    Parameters
    ----------
    rc : RunControl instance
        The rc which has links to the dbs
    dbs: set or None, optional
        The databases to load. If None load all, defaults to None

    Returns
    -------
    client : {FileSystemClient, MongoClient}
        The database client
    """
    if colls is None:
        colls = []
    if rc.client == "fs":
        client = FileSystemClient(rc)
    else:  # we only have one client atm...but may want to change to mongo later
        client = FileSystemClient(rc)
    client.open()
    chained_db = {}
    for db in rc.databases:
        load_database(db, client, rc)
        for base, coll in client.dbs[db["name"]].items():
            if base not in chained_db:
                chained_db[base] = {}
            for k, v in coll.items():
                if k in chained_db[base]:
                    chained_db[base][k].maps.append(v)
                else:
                    chained_db[base][k] = ChainDB(v)
    client.chained_db = chained_db
    return client


@contextmanager
def connect(rc, colls=None):
    """Context manager for ensuring that database is properly setup and torn
    down"""
    client = open_dbs(rc, colls=colls)
    yield client
    for db in rc.databases:
        dump_database(db, client, rc)
    client.close()
