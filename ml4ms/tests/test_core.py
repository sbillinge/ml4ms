import pytest
from ml4ms.core import Dataset

datasets = [
    #test orthonal datasets
    ([{'1': {'_id': 1, 'a': 2}, '3': {'_id': 3, 'b': 4}}, {'2': {'_id': 2}}],
     {'1': {'_id': 1, 'a': 2}, '3': {'_id': 3, 'b': 4}, '2': {'_id': 2}}),
    # test orthogonal items in intersecting sets
    ([{'1': {'_id': 1, 'a': 2}, '3': {'_id': 3, 'b': 4}}, {'3': {'_id': 3, 'c': 5}}],
     {'1': {'_id': 1, 'a': 2}, '3': {'_id': 3, 'b': 4, 'c': 5}}),
    # test overlapping items in intersecting sets
    #...
    # test the new dataset does not have the right primary key (invalid datasets could maybe be tested elsewhere)
    # ...
    # anything else to test?
]
@pytest.mark.parametrize("ds", datasets)
def test_merge_new_data(ds):
    d = Dataset()
    d.dataset = ds[0][0]
    d.merge_new_data(ds[0][1])
    expected = ds[1]
    assert d.dataset == expected
