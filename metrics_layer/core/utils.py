import random
import string
import uuid


def generate_uuid(db_safe=False):
    if db_safe:
        return generate_random_password(40)
    return str(uuid.uuid4())


def generate_random_password(length):
    # Random string with the combination of lower and upper case
    letters = string.ascii_letters
    result_str = "".join(random.choice(letters) for i in range(length))
    return result_str


def flatten_filters(filters: list):
    flat_list = []

    def recurse(filter_obj):
        if isinstance(filter_obj, dict):
            if "conditions" in filter_obj:
                for f in filter_obj["conditions"]:
                    recurse(f)
            else:
                flat_list.append(filter_obj)
        elif isinstance(filter_obj, list):
            for item in filter_obj:
                recurse(item)

    recurse(filters)
    return flat_list
