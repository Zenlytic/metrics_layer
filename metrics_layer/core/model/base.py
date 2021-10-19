import re


class MetricsLayerBase:
    def __init__(self, definition: dict = {}) -> None:
        self._definition = definition

    def __getattr__(self, attr: str):
        return self._definition.get(attr, None)

    def __repr__(self):
        return f"<{self.__class__.__name__} name={self.name}>"

    def to_dict(self):
        return self._definition


class SQLReplacement:
    @staticmethod
    def fields_to_replace(text: str):
        matches = re.finditer(r"\$\{(.*?)\}", text, re.MULTILINE)
        return [match.group(1) for match in matches]
