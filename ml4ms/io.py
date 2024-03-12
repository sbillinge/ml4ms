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


def load_mp_payload(filename):
    """Loads a file that is a list of json documents and returns a dict of its documents

    This is what the materials project payload looks like
    """
    docs = {}
    with open(filename, "r", encoding="utf-8") as fh:
        json_data = fh.read()

    payload = json.loads(json_data)
    for doc in payload:
        if doc.get("date") is None:
            doc["date"] = datetime.date.today()
        else:
            try:
                doc["date"] = datetime.date.fromisoformat(doc["date"])
            except KeyError:
                pass
        doc["_id"] = doc.get("material_id")
        docs.update({doc["_id"]: doc})
    return docs
