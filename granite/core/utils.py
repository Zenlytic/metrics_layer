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


def merge_nested_dict(base: dict, additional: dict):
    for key, val in additional.items():
        if isinstance(val, dict):
            if key in base and isinstance(base[key], dict):
                merge_nested_dict(base[key], additional[key])

        elif isinstance(val, list):
            if key in base and isinstance(base[key], list):
                additional[key] = merge_list(base[key], additional[key])

        else:
            if key in base:
                additional[key] = base[key]

    for key, val in base.items():
        if key not in additional:
            additional[key] = val

    return additional


def merge_list(base_list: list, additional_list: list):
    final_list = []
    added = []
    for item in base_list:
        if not isinstance(item, dict):
            final_list.append(item)
            continue

        additional_item = next((i for i in additional_list if i["name"] == item["name"]), None)
        if additional_item:
            added.append(item["name"])
            final_list.append({**additional_item, **item})
        else:
            final_list.append(item)
    for item in additional_list:
        if isinstance(item, dict) and item["name"] not in added:
            final_list.append(item)
    return final_list
