from .base import GraniteBase


class View(GraniteBase):
    def __init__(self, definition: dict = {}) -> None:

        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "fields"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"View missing required key {k}")

        neither_references = "sql_table_name" not in definition and "derived_table" not in definition
        both_references = "sql_table_name" in definition and "derived_table" in definition

        if neither_references or both_references:
            raise ValueError(
                f"Incorrect table identifiers sql_table_name and derived_table (must have exactly one)"
            )


class Set(GraniteBase):
    def __init__(self, definition: dict = {}) -> None:

        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "fields"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Set missing required key {k}")
