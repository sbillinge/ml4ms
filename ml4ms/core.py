class Dataset:
    def __init__(self):
        self.dataset = {}

    def merge_new_data(self, new_data):
        """
        merge new_data into self.dataset (in place) based on matching "material_id"

        Parameters
        ----------
        new_data: dict
            the first argument is a dictionary with the same format as self.dataset
            {mat_id_1 : {attribute_1: , attribute_2:, ...}, ...}

        Returns
        -------
            nothing
        """
        for key, value in new_data.items():
            self.dataset[key].update(value)


if __name__ == "__main__":
    d = Dataset()
    d.dataset = {'a': {'a': 1, 'b': 2}, 'b': {'a': 3, 'b': 4}}
    actual = d.merge_new_data_into_dataset({'a': {'a': 5, 'b': 6}, 'b': {'a': 7, 'b': 8}})
    expected = {'a': {'a': 5, 'b': 6}, 'b': {'a': 7, 'b': 8}}
    assert actual == expected
    print("Passed all tests!")
