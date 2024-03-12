from copy import deepcopy


def update_schemas(default_schema, user_schema):
    """
    Merging the user schema into the default schema recursively and return the
    merged schema. The default schema and user schema will not be modified
    during the merging.

    Parameters
    ----------
    default_schema : dict
        The default schema.
    user_schema : dict
        The user defined schema.

    Returns
    -------
    updated_schema : dict
        The merged schema.
    """
    updated_schema = deepcopy(default_schema)
    for key in user_schema.keys():
        if (
            (key in updated_schema)
            and isinstance(updated_schema[key], dict)
            and isinstance(user_schema[key], dict)
        ):
            updated_schema[key] = update_schemas(updated_schema[key], user_schema[key])
        else:
            updated_schema[key] = user_schema[key]

    return updated_schema
