from .base import MetricsLayerBase


class DashboardLayouts:
    grid = "grid"


class DashboardElement(MetricsLayerBase):
    def __init__(self, definition: dict = {}, project=None) -> None:

        self.project = project
        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["model", "explore"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Dashboard Element missing required key {k}")


class Dashboard(MetricsLayerBase):
    def __init__(self, definition: dict = {}, project=None) -> None:
        if definition.get("name") is not None:
            definition["name"] = definition["name"].lower()

        if definition.get("layout") is None:
            definition["layout"] = DashboardLayouts.grid

        self.project = project
        self.validate(definition)
        super().__init__(definition)

    @property
    def label(self):
        if self._definition.get("label"):
            return self._definition.get("label")
        return self.name.replace("_", " ").title()

    def validate(self, definition: dict):
        required_keys = ["name", "layout"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Dashboard missing required key {k}")

    def printable_attributes(self):
        to_print = ["name", "label", "description"]
        attributes = self.to_dict()
        attributes["type"] = "dashboard"
        return {key: attributes.get(key) for key in to_print if attributes.get(key) is not None}

    def elements(self):
        elements = self._definition.get("elements", [])
        return [DashboardElement(e, project=self.project) for e in elements]
