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
    print(rc.client["test_db"]["test_coll"].get("id1"))


if __name__ == "__main__":
    main()
