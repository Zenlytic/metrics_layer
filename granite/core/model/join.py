from .base import GraniteBase


class Join(GraniteBase):
    def __init__(self, definition: dict = {}, view=None, explore=None) -> None:

        self.validate(definition)
        self.view = view
        self.explore = explore
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "relationship", "type"]

        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Join missing required key {k}")

        neither_join_keys = "sql_on" not in definition and "foreign_key" not in definition
        both_join_keys = "sql_on" in definition and "foreign_key" in definition

        if both_join_keys or neither_join_keys:
            raise ValueError(f"Incorrect join identifiers sql_on and foreign_key (must have exactly one)")
