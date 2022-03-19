from .base import MetricsLayerBase


class AccessGrant(MetricsLayerBase):
    def __init__(self, definition: dict = {}) -> None:

        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "user_attribute", "allowed_values"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Access Grant missing required key {k}")


class Model(MetricsLayerBase):
    def __init__(self, definition: dict = {}) -> None:

        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "connection"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Model missing required key {k}")

    def collect_errors(self):
        if not self.valid_name(self.name):
            return [self.name_error("model", self.name)]
        return []

    def printable_attributes(self):
        to_print = ["name", "type", "label", "group_label", "connection", "explore_names"]
        attributes = self.to_dict()
        attributes["explore_names"] = [e["name"] for e in attributes["explores"]]
        return {key: attributes.get(key) for key in to_print if attributes.get(key) is not None}

    @property
    def explores(self):
        return self._definition.get("explores", [])

    @property
    def access_grants(self):
        return self._definition.get("access_grants", [])
