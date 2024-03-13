import copy
import json
from pathlib import Path

import jsonschema


def load_schemas():
    here = Path(__file__).parent
    schema_file = here / "schemas.json"
    with open(schema_file, "r", encoding="utf-8") as schema_file:
        SCHEMAS = json.load(schema_file)
    return SCHEMAS


def load_exemplars():
    here = Path(__file__).parent
    exemplars_file = here / "exemplars.json"
    with open(exemplars_file, "r", encoding="utf-8") as exemplars_file:
        EXEMPLARS = json.load(exemplars_file)
    return EXEMPLARS


def validate(colltype, record, schemas):
    """Validate a record for a given db

    Parameters
    ----------
    colltype : str
        The name of the db in question
    record : dict
        The record to be validated
    schemas : dict
        The schema to validate against

    Returns
    -------
    rtn : bool
        True is valid
    errors: dict
        The errors encountered (if any)

    """

    if colltype in schemas:
        schema = copy.deepcopy(schemas[colltype])
        # v = NoDescriptionValidator(schema)
        return jsonschema.validate(instance=record, schema=schema)
    else:
        return True, ()
