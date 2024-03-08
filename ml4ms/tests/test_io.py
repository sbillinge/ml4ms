import datetime
import json
import tempfile
from pathlib import Path

import pytest

from ml4ms.io import load_json

datasets = [
    (
        {"first": {"date": "2021-05-01", "name": "me", "test_list": [5, 4]}, "second": {}},
        {
            "first": {"_id": "first", "name": "me", "date": datetime.date(2021, 5, 1), "test_list": [5, 4]},
            "second": {"_id": "second"},
        },
    ),
]


@pytest.mark.parametrize("json_doc, expected", datasets)
def test_load_json(json_doc, expected):
    temp_dir = Path(tempfile.gettempdir())
    filename = temp_dir / "test.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(json_doc, f)
    actual = load_json(filename)
    assert actual == expected
