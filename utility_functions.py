import datetime
import json
import os


def get_current_datetime():
    return (
        datetime.datetime.utcnow()
        .replace(tzinfo=datetime.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("T", " ")
    )


def get_or_create_json_list(file_path):
    """
    Get a list from a JSON file or create one if the file does not exist.

    Returns:
        dict: The list of crawls, read from the JSON file
    """
    if os.path.isfile(file_path):
        return load_json(file_path)
    else:
        return []


def load_json(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        data = file.read()
        return json.loads(data)


def store_json(file_path, dictionary):
    output_file = open(file_path, "w", encoding="utf-8")
    json.dump(
        dictionary,
        output_file,
        indent=4,
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    )
    output_file.close()


def check_json_validity(json_dict, key_type_tuples):
    """
    Checks if the given object contains all the neccessary keys and if all the values
    corresponding to these keys have the correct type.

    Args:
        json_dict (dict): Dictionary representing the JSON object
        key_type_tuples (list): Tuples of a key that should be contained in the dict
            and the type its associated value should have.

    Raises:
        Exception: Raised if one of the keys is missing from the dict or the values
            does not have the correct type.
    """
    for (key, value_type) in key_type_tuples:
        if key not in json_dict or type(json_dict[key]) != value_type:
            raise Exception(
                f"The property {key} is either missing or not a {value_type}"
            )
