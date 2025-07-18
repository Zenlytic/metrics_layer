import difflib
import re
from typing import Callable, Iterable, TypeVar, overload

from metrics_layer.core.exceptions import QueryError

NAME_REGEX = re.compile(r"([A-Za-z0-9\_]+)")

T = TypeVar("T")


class MetricsLayerBase:
    def __init__(self, definition: dict = {}) -> None:
        self._definition = definition

    def __getattr__(self, attr: str):
        return self._definition.get(attr, None)

    def __repr__(self):
        return f"<{self.__class__.__name__} name={self.name}>"

    def to_dict(self):
        return {**self._definition}

    @staticmethod
    @overload
    def normalize_name(name: str) -> str: ...

    @staticmethod
    @overload
    def normalize_name(name: T) -> T: ...

    @staticmethod
    def normalize_name(name):
        if isinstance(name, str):
            return name.lower()
        return name

    @staticmethod
    def valid_name(name: str):
        match = re.match(NAME_REGEX, name)
        if match is None:
            return False
        return match.group(1) == name

    @staticmethod
    def name_error(entity_name: str, name: str):
        return (
            f"{entity_name.title()} name: {name} is invalid. Please reference "
            "the naming conventions (only letters, numbers, or underscores)"
        )

    @staticmethod
    def _raise_query_error_from_cte(field_name: str):
        raise QueryError(
            f"Field {field_name} is not present in either source query, so it"
            " cannot be applied as a filter. Please add it to one of the source queries."
        )

    @staticmethod
    def line_col(element):
        line = getattr(getattr(element, "lc", None), "line", None)
        column = getattr(getattr(element, "lc", None), "col", None)
        return line, column

    @staticmethod
    def invalid_property_error(
        definition: dict, valid_properties: Iterable[str], entity_name: str, name: str, error_func: Callable
    ):
        errors = []
        for key in definition:
            if key not in valid_properties:
                proposed_property = MetricsLayerBase.propose_property(key, valid_properties)
                proposed = f" Did you mean {proposed_property}?" if proposed_property else ""
                errors.append(
                    error_func(
                        definition[key],
                        (
                            f"Property {key} is present on {entity_name.title()} {name}, but it is not a"
                            f" valid property.{proposed}"
                        ),
                    )
                )
        return errors

    @staticmethod
    def field_name_parts(field_name: str):
        if field_name.count(".") == 1:
            view_name, name = field_name.split(".")
        else:
            view_name, name = None, field_name
        return view_name, name

    @staticmethod
    def propose_property(invalid_property_name: str, valid_properties: Iterable[str]) -> str:
        closest_match = difflib.get_close_matches(invalid_property_name, valid_properties, n=1)
        if closest_match:
            return closest_match[0]
        else:
            return ""


class SQLReplacement:
    @staticmethod
    def fields_to_replace(text: str):
        matches = re.finditer(r"\$\{(.*?)\}", text, re.MULTILINE)
        return [match.group(1) for match in matches]
