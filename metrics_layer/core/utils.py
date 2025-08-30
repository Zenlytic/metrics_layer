import hashlib
import json
import random
import string
import uuid
from typing import Any


def generate_uuid(db_safe=False):
    if db_safe:
        return generate_random_password(40)
    return str(uuid.uuid4())


def generate_random_password(length):
    # Random string with the combination of lower and upper case
    letters = string.ascii_letters
    result_str = "".join(random.choice(letters) for i in range(length))
    return result_str


def flatten_filters(filters: list, return_nesting_depth: bool = False):
    nesting_depth = 0
    flat_list = []

    def recurse(filter_obj, return_nesting_depth: bool):
        nonlocal nesting_depth
        if isinstance(filter_obj, dict):
            if "conditions" in filter_obj:
                nesting_depth += 1
                for f in filter_obj["conditions"]:
                    recurse(f, return_nesting_depth)
            else:
                if return_nesting_depth:
                    filter_obj["nesting_depth"] = nesting_depth
                flat_list.append(filter_obj)
        elif isinstance(filter_obj, list):
            nesting_depth += 1
            for item in filter_obj:
                recurse(item, return_nesting_depth)

    recurse(filters, return_nesting_depth=return_nesting_depth)
    return flat_list


def compute_combined_sql_md5(sql: str, type: str, filters: Any, non_additive_dimension: Any) -> str:
    hash_components = {
        "sql": sql,
        "type": type,
        "filters": _normalize_filters(filters),
        "non_additive_dimension": _normalize_non_additive_dimension(non_additive_dimension),
    }

    hash_input = json.dumps(hash_components, sort_keys=True)
    result = hashlib.md5(hash_input.encode("utf-8"))  # nosec
    return result.hexdigest()


def _normalize_filters(filters: Any) -> list:
    if filters is None:
        return []

    if isinstance(filters, dict) and "values" in filters:
        return filters.get("values", [])

    if isinstance(filters, list):
        return filters

    return []


def _normalize_non_additive_dimension(non_additive_dimension: Any) -> dict:
    if non_additive_dimension is None:
        return {}

    if isinstance(non_additive_dimension, dict):
        return non_additive_dimension

    return {}
