import pytest

from ml4ms.core import merge_new_data
from ml4ms.database import FileSystemClient
from ml4ms.runcontrol import RunControl

# datasets in format [(original, to_merge, expected), ()]
datasets = [
    # test identical datasets
    (
        {"1": {"_id": "1", "a": 2}, "3": {"_id": "3"}},
        {"1": {"_id": "1", "a": 2}, "3": {"_id": "3"}},
        {"1": {"_id": "1", "a": 2}, "3": {"_id": "3"}},
    ),
    # test orthogonal items
    (
        {"1": {"_id": "1", "a": 2}, "3": {"_id": "3"}},
        {"3": {"_id": "3", "b": 4}},
        {"1": {"_id": "1", "a": 2}, "3": {"_id": "3", "b": 4}},
    ),
    # test overlapping items in intersecting sets
    (
        {"1": {"_id": "1", "a": 2}, "3": {"_id": "3"}},
        {"1": {"_id": "1", "a": 3, "b": 3}},  # this should overwrite a['1']
        {"1": {"_id": "1", "a": 3, "b": 3}, "3": {"_id": "3"}},
    ),
    # test merging an empty collection
    ({"1": {"_id": "1", "a": 2}, "3": {"_id": "3"}}, {}, {"1": {"_id": "1", "a": 2}, "3": {"_id": "3"}}),
    # test the new dataset does not have the right primary key (invalid datasets could maybe be tested elsewhere)
    (
        {"1": {"_id": "1", "a": 2}, "3": {"_id": "3"}},
        {"new_key": {"_id": "new_key"}},
        {"1": {"_id": "1", "a": 2}, "3": {"_id": "3"}, "new_key": {"_id": "new_key"}},
    )
    # anything else to test?
]


@pytest.mark.parametrize("old_ds, new_ds, expected", datasets)
def test_merge_new_data(old_ds, new_ds, expected):
    rc = RunControl()
    rc.client = FileSystemClient(rc)
    rc.client.dbs["test_db"]["test_coll"] = old_ds
    merge_new_data(rc, "test_db", "test_coll", new_ds)
    assert rc.client.dbs["test_db"]["test_coll"] == expected
