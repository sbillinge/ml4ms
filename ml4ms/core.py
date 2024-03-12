import datetime
import uuid

from ml4ms.runcontrol import NotSpecified

KNOWN_MODELS = ["random_forest"]
KNOWN_FEATURE_TYPES = ["spectrum", "crystal_structure"]


class Trial:
    """A container for each trial of a materials ML campaign"""

    def __init__(
        self,
        rc,
        trial_descr="",
        _updaters=None,
        data_sample_filters=None,
        feature_filters=None,
        models_list=None,
        metadata=None,
    ):
        f"""Trial constructor

        Parameters
        ----------
        rc: ml4ms.rc.RunControl object
          The run control object
        trial_descr: str (optional)
          The place to put a description of the trial if you want
        self._id: str
          The UUID of the trial instance
        self.data_sample_filters: list of dicts
          The list of pymongo filters that will be used to sample the data in the database for this trial.
          For example, to get all structures containing Ti it would be ['absorbing_element': 'Ti']
        self.feature_filters: list of strings
          The filters that will be used to pull features from the sampled database items.  Features pulled must
          belong to known feature types {*KNOWN_FEATURE_TYPES, }, e.g., ['spectrum', 'structure'] will use both
          xanes spectrum and crystal_structure as features
        self.models_list: list of strings
          The models that will be tried from {KNOWN_MODELS}, e.g., ['random_forest']
        self.metadata: dict
          The additional metadata to run. e.g., {{"pdf": {{"qmax": 25}}}}
        """

        self.rc = rc
        self._id = str(uuid.uuid4())
        self.user_name = rc.user_name
        self.user_email = rc.user_email
        self._updaters = _updaters
        self.trial_descr = trial_descr
        if metadata is None:
            metadata = {}
        self.metadata = metadata
        if data_sample_filters is None:
            data_sample_filters = [{}]
        self.data_sample_filters = data_sample_filters
        if models_list is None:
            models_list = []
        self.models_list = models_list
        if feature_filters is None:
            feature_filters = []
        self.feature_filters = feature_filters

    def _update(self, other):
        """Updates the trial with values from a mapping (dict).

        If this trial has a key in self, other, and self._updaters, then the updaters
        value is called to perform the update.  Otherwise, a new key is added
        as an attribute and the value assigned to this attribute.

        This function should return a copy to be safe and not update in-place.
        """
        if hasattr(other, "_dict"):
            other = other._dict
        elif not hasattr(other, "items"):
            other = dict(other)
        for k, v in other.items():
            if v is NotSpecified:
                pass
            elif k in self._updaters and k in self:
                v = self._updaters[k](getattr(self, k), v)
            setattr(self, k, v)

    def serialize_trial(self):
        """JSON serializer for objects not serializable by default json code"""

        if isinstance(self, datetime.date):
            serial = self.isoformat()
            return serial
        if isinstance(self, datetime.datetime):
            serial = self.isoformat()
            return serial
        return self.__dict__

    def dump_trial(self, db=None, collection=None):
        """dumps the trial object to collection rc.trial_coll collection in database db"""
        if db is None:
            db = self.rc.client.db
        if collection is None:
            collection = self.rc.trial_collection
        self.datetime = datetime.datetime.now()
        self.rc.client.insert_one(collection, self.serialize_trial(), db=db)


def clone_trial(rc, collection=None, trial_id=None, db=None):
    """Serializes the trial object to collection rc.trial_coll collection in database db"""
    if db is None:
        db = rc.client.db
    if collection is None:
        collection = rc.trial_collection
    trial_json = rc.client.find_one(collection, {"_id": trial_id}, db=db)
    return Trial(**trial_json)


def merge_new_data(rc, coll, new_coll, db_info=None):
    if db_info is None:
        db_info = rc.database_info
    dbname = db_info.get("name")
    for key, doc in new_coll.items():
        if doc.get("_id") is None:
            doc["_id"] = key
        if doc.get("_id") in rc.client[dbname][coll].keys():
            rc.client.update_one(coll, {"_id": doc["_id"]}, doc)
        else:
            rc.client.insert_one(coll, doc)


def clone_collection(rc, db, existing_coll, new_coll_name=None):
    # if new_coll_name is None:
    #    new_coll_name =
    # rc.client.
    pass
