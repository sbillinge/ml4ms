import copy
import os
from argparse import ArgumentParser
from pathlib import Path

from ml4ms.core import merge_new_data
from ml4ms.database import connect
from ml4ms.io import load_mp_payload
from ml4ms.runcontrol import DEFAULT_RC, load_rcfile
from ml4ms.schemas import load_schemas, validate
from ml4ms.tools import update_schemas


def create_parser():
    p = ArgumentParser()
    p.add_argument(
        "--ingest", help="filename and path of pure json format file to add to our database", default=None
    )
    p.add_argument("--validate", nargs="+", help="validate the given collection against the schema", default=None)
    return p


# def validate(rc):
#     """Validate the combined database against the schemas"""
#     from ml4ms.schemas import validate
#
#     print("=" * 10 + "\nVALIDATING\n")
#     any_errors = False
#     if getattr(rc, "collection"):
#         db = {rc.collection: rc.client.chained_db[rc.collection]}
#     else:
#         db = rc.client.chained_db
#     # loop over all the collections in the db
#     for name, collection in db.items():
#         errored_print = False
#         for doc in collection:
#             v = validate(doc.get("schema"), doc, rc.schemas)
#             if v[0] is False:
#                 if errored_print is False:
#                     errored_print = True
#                     any_errors = True
#                     print(f"Errors found in {name}")
#                     print("=" * len(f"Errors found in {name}"))
#                 print(f"ERROR in {doc_id}:")
#                 pprint(v[1])
#                 cap = copy(v[1])
#                 for vv in v[1]:
#                     pprint(doc.get(vv))
#                 print("-" * 15)
#                 print("\n")
#     if not any_errors:
#         print("\nNO ERRORS IN DBS\n" + "=" * 15)
#     else:
#         # uncomment when debugging scheme errors
#         #
#         # sys.exit(f"Validation failed on some records\n {cap}")
#         sys.exit(f"Validation failed on some records")


def main(rc, args=None):
    rc = copy.copy(DEFAULT_RC)
    try:
        rc._update(load_rcfile(rc.user_config))
    except AttributeError:
        raise AttributeError(
            f"ERROR: couldn't find user.  Please create file {rc.user_config} with, at "
            f"least, {{'user_name': '<your name>', 'user_email': '<your email>'}}"
        )
    print(rc.__dict__)
    if rc.__dict__.get("user_name") is None:
        raise AttributeError(
            f"ERROR: couldn't find user_name.  Please add "
            f"{{'user_name': '<your name>', 'user_email': '<your email>'}} to your user.json "
            f"file: {rc.user_config}"
        )
    if rc.__dict__.get("user_email") is None:
        raise AttributeError(
            f"ERROR: couldn't find user_email.  Please add "
            f"{{'user_name': '<your name>', 'user_email': '<your email>'}} to your user.json "
            f"file: {rc.user_config}"
        )
    if os.path.exists("ml4msrc.json"):
        rc._update(load_rcfile("ml4msrc.json"))
    parser = create_parser()
    args = parser.parse_args()
    rc._update(args.__dict__)
    if "schemas" in rc._dict:
        user_schema = copy.deepcopy(rc.schemas)
        default_schema = copy.deepcopy(load_schemas())
        rc.schemas = update_schemas(default_schema, user_schema)
    else:
        rc.schemas = load_schemas()
    if rc.validate is not None:
        validate(rc.validate)
        exit()
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
    db = rc.client.db
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
