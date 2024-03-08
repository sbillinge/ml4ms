import datetime

EXEMPLARS = {
    "test_coll": {"_id": "first_doc", "name": "me", "date": datetime.date(2021, 5, 1), "test_list": [5, 4]},
    "test_coll2": {"_id": "second_doc"},
}
SCHEMAS = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://example.com/product.schema.json",
    "title": "ml4ms document",
    "description": "All the things for defining a material",
    "type": "object",
    "properties": {
        "_id": {"type": "string", "description": "a universally unique identifier"},
        "owner": {"type": "string", "description": "the person responsible for the material"},
        "datetime": {"type": "string", "description": "timestamp for the object"},
        "descriptors": {"type": "list"},
    },
}
