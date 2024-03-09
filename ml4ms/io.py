import datetime
import json


def load_json(filename):
    """Loads a JSON file and returns a dict of its documents.

    Expects one document per line in the file with the form:
    {'_id': '<id>', 'field1':'value1', 'field2':'value2'}
    """
    docs = {}
    with open(filename, "r", encoding="utf-8") as fh:
        docs = json.load(fh)
    for key, doc in docs.items():
        try:
            doc["date"] = datetime.date.fromisoformat(doc["date"])
        except KeyError:
            pass
        doc["_id"] = key
    return docs
