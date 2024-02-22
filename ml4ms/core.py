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
