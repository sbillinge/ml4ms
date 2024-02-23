import pytest

from ml4ms.core import Dataset

# datasets in format [(original, to_merge, expected), ()]
datasets = [
    # test identical datasets
    (
        {"1": {"_id": 1, "a": 2}, "3": {"_id": 3}},
        {"1": {"_id": 1, "a": 2}, "3": {"_id": 3}},
        {"1": {"_id": 1, "a": 2}, "3": {"_id": 3}},
    ),
    # test orthogonal items
    (
        {"1": {"_id": 1, "a": 2}, "3": {"_id": 3}},
        {"3": {"_id": 3, "b": 4}},
        {"1": {"_id": 1, "a": 2}, "3": {"_id": 3, "b": 4}},
    ),
    # test overlapping items in intersecting sets
    (
        {"1": {"_id": 1, "a": 2}, "3": {"_id": 3}},
        {"1": {"_id": 1, "a": 3, "b": 3}},  # this should overwrite a['1']
        {"1": {"_id": 1, "a": 3, "b": 3}, "3": {"_id": 3}},
    ),
    # test merging an empty collection
    ({"1": {"_id": 1, "a": 2}, "3": {"_id": 3}}, {}, {"1": {"_id": 1, "a": 2}, "3": {"_id": 3}}),
    # test the new dataset does not have the right primary key (invalid datasets could maybe be tested elsewhere)
    (
        {"1": {"_id": 1, "a": 2}, "3": {"_id": 3}},
        {"invalid_key": {"_id": "invalid_key"}},
        {"1": {"_id": 1, "a": 2}, "3": {"_id": 3}},
    )
    # anything else to test?
]


@pytest.mark.parametrize("old_ds, new_ds, expected", datasets)
def test_merge_new_data(old_ds, new_ds, expected):
    d = Dataset()
    d.dataset = old_ds
    d.merge_new_data(new_ds)
    assert d.dataset == expected
