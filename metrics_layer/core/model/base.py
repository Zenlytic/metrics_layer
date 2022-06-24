import re

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
        return match.group(1) == name

    @staticmethod
    def name_error(entity_name: str, name: str):
        return (
            f"{entity_name.title()} name: {name} is invalid. Please reference "
            "the naming conventions (only letters, numbers, or underscores)"
        )

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


class SQLReplacement:
    @staticmethod
    def fields_to_replace(text: str):
        matches = re.finditer(r"\$\{(.*?)\}", text, re.MULTILINE)
        return [match.group(1) for match in matches]
