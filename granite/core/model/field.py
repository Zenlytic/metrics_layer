from .base import GraniteBase


class Field(GraniteBase):
    def __init__(self, definition: dict = {}, view=None) -> None:
        self.defaults = {"type": "string", "primary_key": "no"}

        self.validate(definition)
        self.view = view
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "field_type", "sql"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Field missing required key {k}")
