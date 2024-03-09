# class Dataset:
#     def __init__(self):
#         self.dataset = {}
#
#     def merge_new_data(self, new_data):
#         """
#         merge new_data into self.dataset (in place) based on matching "material_id"
#
#         Parameters
#         ----------
#         new_data: dict
#             The first argument is a dictionary with the same format as self.dataset
#             {mat_id_1 : {attribute_1: , attribute_2:, ...}, ...}
#
#         Returns
#         -------
#             nothing
#         """
#         for key, value in new_data.items():
#             # merge only if key exists
#             if key in self.dataset:
#                 self.dataset[key].update(value)


def merge_new_data(rc, coll, new_coll, db=None):
    if db is None:
        db = rc.databases[0]
    dbname = db.get("name")
    for key, doc in new_coll.items():
        if doc.get("_id") is None:
            doc["_id"] = key
        if doc.get("_id") in rc.client[dbname][coll].keys():
            rc.client.update_one(coll, {"_id": doc["_id"]}, doc, db=db)
        else:
            rc.client.insert_one(coll, doc, db=db)


def clone_collection(rc, db, existing_coll, new_coll_name=None):
    # if new_coll_name is None:
    #    new_coll_name =
    # rc.client.
    pass
