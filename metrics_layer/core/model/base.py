import difflib
import re
from typing import List

NAME_REGEX = re.compile(r"([A-Za-z0-9\_]+)")


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
    def line_col(element):
        line = getattr(getattr(element, "lc", None), "line", None)
        column = getattr(getattr(element, "lc", None), "col", None)
        return line, column

    @staticmethod
    def invalid_property_error(
        definition: dict, valid_properties: List[str], entity_name: str, name: str, error_func: callable
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
        explore_name, view_name = None, None
        if field_name.count(".") == 2:
            explore_name, view_name, name = field_name.split(".")
        elif field_name.count(".") == 1:
            view_name, name = field_name.split(".")
        else:
            name = field_name
        return explore_name, view_name, name

    @staticmethod
    def propose_property(invalid_property_name: str, valid_properties: List[str]) -> str:
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
