from .base import MetricsLayerBase


class DashboardLayouts:
    grid = "grid"


class DashboardElement(MetricsLayerBase):
    def __init__(self, definition: dict = {}, dashboard=None, project=None) -> None:

        self.project = project
        self.dashboard = dashboard
        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["model", "explore"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Dashboard Element missing required key {k}")

    @property
    def slice_by(self):
        return self._definition.get("slice_by", [])

    def collect_errors(self):
        errors = []

        if not self._function_executes(self.project.get_model, self.model):
            err_msg = f"Could not find model {self.model} referenced in dashboard {self.dashboard.name}"
            errors.append(err_msg)

        if not self._function_executes(self.project.get_explore, self.explore):
            err_msg = (
                f"Could not find explore {self.explore} in model {self.model} "
                f"referenced in dashboard {self.dashboard.name}"
            )
            errors.append(err_msg)

        for field in self.slice_by:
            if not self._function_executes(self.project.get_field, field, explore_name=self.explore):
                err_msg = (
                    f"Could not find field {field} in explore {self.explore} "
                    f"referenced in dashboard {self.dashboard.name}"
                )
                errors.append(err_msg)
        return errors

    @staticmethod
    def _function_executes(func, argument, **kwargs):
        try:
            func(argument, **kwargs)
            return True
        except Exception:
            return False


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

    def collect_errors(self):
        errors = []
        for element in self.elements():
            errors.extend(element.collect_errors())
        return errors

    def printable_attributes(self):
        to_print = ["name", "label", "description"]
        attributes = self.to_dict()
        attributes["type"] = "dashboard"
        return {key: attributes.get(key) for key in to_print if attributes.get(key) is not None}

    def elements(self):
        elements = self._definition.get("elements", [])
        return [DashboardElement(e, dashboard=self, project=self.project) for e in elements]
