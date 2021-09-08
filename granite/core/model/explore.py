from .base import GraniteBase


class Explore(GraniteBase):
    def __init__(self, definition: dict = {}) -> None:

        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "model"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Explore missing required key {k}")
