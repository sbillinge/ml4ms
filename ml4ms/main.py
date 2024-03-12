import copy
import os
from argparse import ArgumentParser
from pathlib import Path

from ml4ms.core import merge_new_data
from ml4ms.database import connect
from ml4ms.io import load_mp_payload
from ml4ms.runcontrol import DEFAULT_RC, load_rcfile


def create_parser():
    p = ArgumentParser()
    p.add_argument(
        "--ingest", help="filename and path of pure json format file to add to our database", default=None
    )
    return p


def main(args=None):
    rc = copy.copy(DEFAULT_RC)
    parser = create_parser()
    args = parser.parse_args()
    if os.path.exists(rc.user_config):
        rc._update(load_rcfile(rc.user_config))
    if os.path.exists("ml4msrc.json"):
        rc._update(load_rcfile("ml4msrc.json"))
    rc._update(args.__dict__)
    with connect(rc, colls=None) as rc.client:
        tricode(rc)
    return rc


def tricode(rc):
    """
    This is where you will put your code. It could be in a different module and just get imported here.  We can
    (and should) change the name of course, but this is just an example so you can see how it works.

    It is set up rn so we can have more than one db, but we will probably only be using it with one database.  We
    can decide if we want to simplify it in this way, in which case we wouldn't have to reference the db always.

    Note that updates, inserts etc. modify the database.  We need to be careful how we handle this as it will make
    your code behave differently each time you run it.  We would need to separate tasks of building the database
    and the subsequent training.  I think it can work well, but it means working a bit differently than we do now.

    Although the infrastructure allows connecting to more than one db, I think we will only ever be connecting
    to one, so code below is written on that basis (active db is always rc.databases[0])
    """
    client = rc.client
    rc.db = rc.databases[0]
    db = rc.client[rc.db.get("name")]
    if rc.ingest is not None:
        datafile = Path(rc.ingest)
        docs = load_mp_payload(datafile)
        merge_new_data(rc, rc.colls[0], docs)

    # some example code:
    #     print("print the collection called test_coll from the test_db database")
    #     print(db["coll1"])

    # get a document using its id
    print("\n")
    print("get id1 by using its id")
    found_doc = db["coll1"].get("mp-5020")
    print(found_doc)

    # find a document by a filter
    print("\n")
    print("find id1 by filtering for the (first) doc with name test1")
    found_doc = client.find_one("coll1", {"_id": "mp-5020"})
    print(found_doc)

    # update something in an existing doc
    # print("\n")
    # print("update id1 after finding it with the filter")
    # client.update_one("test_coll", {"name": "test1"}, {"description": "updated description"})
    # print(client["tri_db"]["coll1"])


if __name__ == "__main__":
    main()
