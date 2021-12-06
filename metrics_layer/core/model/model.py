from .base import MetricsLayerBase


class Model(MetricsLayerBase):
    def __init__(self, definition: dict = {}) -> None:

        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "connection"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Explore missing required key {k}")

    def printable_attributes(self):
        to_print = ["name", "type", "label", "group_label", "connection", "explore_names"]
        attributes = self.to_dict()
        attributes["explore_names"] = [e["name"] for e in attributes["explores"]]
        return {key: attributes.get(key) for key in to_print if attributes.get(key) is not None}
