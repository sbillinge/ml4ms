class Dataset:
    def __init__(self):
        self.dataset = {}

    def merge_new_data(self, new_data):
        """
        merge new_data into self.dataset (in place) based on matching "material_id"

        Parameters
        ----------
        new_data: dict
            The first argument is a dictionary with the same format as self.dataset
            {mat_id_1 : {attribute_1: , attribute_2:, ...}, ...}

        Returns
        -------
            nothing
        """
        for key, value in new_data.items():
            # merge only if key exists
            if key in self.dataset:
                self.dataset[key].update(value)


def merge_new_data(rc, db, coll, new_coll):
    for doc in new_coll.values():
        if doc.get("_id") in rc.client[db][coll].keys():
            rc.client.update_one(db, coll, {"_id": doc["_id"]}, doc)
        else:
            rc.client.insert_one(db, coll, doc)


def clone_collection(rc, db, existing_coll, new_coll_name=None):
    # if new_coll_name is None:
    #    new_coll_name =
    # rc.client.
    pass
