import copy
import json
from pathlib import Path

import jsonschema

from ml4ms.fsclient import date_encoder


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
        exemplars = json.load(exemplars_file)
    return exemplars


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
        # print(f"Validating {colltype} with schema {schemas.get(colltype)}")
        schema = copy.deepcopy(schemas.get(colltype))
        for key, value in record.items():
            record[key] = date_encoder(value)
        # v = NoDescriptionValidator(schema)
        jsonschema.validate(instance=record, schema=schema)
    # else:
    #     return True, ()
