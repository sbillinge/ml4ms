import datetime
import tempfile
from pathlib import Path

import pytest

from ml4ms.fsclient import date_encoder, dump_json_collection, load_json_collection


def test_date_encoder():
    day = datetime.date(2021, 1, 1)
    time = datetime.datetime(2021, 5, 18, 6, 28, 21, 504549)
    assert date_encoder(day) == "2021-01-01"
    assert date_encoder(time) == "2021-05-18T06:28:21.504549"


def test_dump_json_collection():
    doc = {
        "first": {"_id": "first", "name": "me", "date": datetime.date(2021, 5, 1), "test_list": [5, 4]},
        "second": {"_id": "second"},
    }
    json_doc = '{"_id": "first", "date": "2021-05-01", "name": "me", "test_list": [5, 4]}\n{"_id": "second"}'
    temp_dir = Path(tempfile.gettempdir())
    filename = temp_dir / "test.json"
    dump_json_collection(filename, doc, date_handler=date_encoder)
    with open(filename, "r", encoding="utf-8") as f:
        actual = f.read()
    assert actual == json_doc


datasets = [
    (
        ('{"_id": "first", "date": "2021-05-01", "name": "me", "test_list": [5, 4]}\n{"_id": "second"}'),
        {
            "first": {"_id": "first", "name": "me", "date": datetime.date(2021, 5, 1), "test_list": [5, 4]},
            "second": {"_id": "second"},
        },
    ),
]


@pytest.mark.parametrize("json_col, expected", datasets)
def test_load_json_collection(json_col, expected):
    temp_dir = Path(tempfile.gettempdir())
    filename = temp_dir / "test.json"
    with open(filename, "w", encoding="utf-8") as f:
        f.writelines(json_col)
    actual = load_json_collection(filename)
    assert actual == expected
