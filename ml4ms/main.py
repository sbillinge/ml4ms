import copy
import os
from argparse import ArgumentParser

from ml4ms.database import connect
from ml4ms.runcontrol import DEFAULT_RC, load_rcfile


def create_parser():
    p = ArgumentParser()
    p.add_argument("--test", action="store_true")
    return p


def main(args=None):
    rc = copy.copy(DEFAULT_RC)
    parser = create_parser()
    args = parser.parse_args()
    # if os.path.exists(rc.user_config):
    #     rc._update(load_rcfile(rc.user_config))
    if os.path.exists("ml4msrc.json"):
        rc._update(load_rcfile("ml4msrc.json"))
    rc._update(args.__dict__)
    if args.test:
        print(rc.__dict__)
        return
    # print(f"before: {rc.__dict__}")
    with connect(rc, colls=None) as rc.client:
        tinascode(rc)
    return rc


def tinascode(rc):
    """
    This is where you will put your code. It could be in a different module and just get imported here.  We can
    (and should) change the name of course, but this is just an example so you can see how it works.

    It is set up rn so we can have more than one db, but we will probably only be using it with one database.  We
    can decide if we want to simplify it in this way, in which case we wouldn't have to reference the db always.

    Note that updates, inserts etc. modify the database.  We need to be careful how we handle this as it will make
    your code behave differently each time you run it.  We would need to separate tasks of building the database
    and the subsequent training.  I think it can work well, but it means working a bit differently than we do now.
    """
    client = rc.client
    print("print the collection called test_coll from the test_db database")
    print(client["test_db"]["test_coll"])

    # add a new doc to test_coll
    print("\n")
    print("test_coll after the new doc is added")
    newdoc = {"_id": "id_new", "name": "name_new", "newthing": "new_newthing"}
    client.insert_one("test_db", "test_coll", newdoc)
    print(client["test_db"]["test_coll"])

    # find a document by a filter
    print("\n")
    print("find id1 by filtering for the (first) doc with name test1")
    found_doc = client.find_one("test_db", "test_coll", {"name": "test1"})
    print(found_doc)

    # update something in an existing doc
    print("\n")
    print("update id1 after finding it with the filter")
    client.update_one("test_db", "test_coll", {"name": "test1"}, {"description": "updated description"})
    print(client["test_db"]["test_coll"])


if __name__ == "__main__":
    main()
