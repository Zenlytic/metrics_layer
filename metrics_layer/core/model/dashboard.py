import json
from typing import TYPE_CHECKING

from metrics_layer.core.exceptions import (
    AccessDeniedOrDoesNotExistException,
    QueryError,
)

from .base import MetricsLayerBase
from .filter import Filter

if TYPE_CHECKING:
    from metrics_layer.core.model.project import Project


class DashboardLayouts:
    grid = "grid"


class DashboardElement(MetricsLayerBase):
    def __init__(self, definition: dict, dashboard, project) -> None:
        self.project: Project = project
        self.dashboard = dashboard
        self.validate(definition)
        super().__init__(definition)

    def get_model(self):
        model = self.project.get_model(self.model)
        if model is None:
            raise QueryError(
                f"Could not find model {self.model} referenced in dashboard {self.dashboard.name}."
            )
        return model

    def validate(self, definition: dict):
        required_keys = ["model"]
        for k in required_keys:
            if k not in definition:
                raise QueryError(f"Dashboard Element missing required key {k}")

    def to_dict(self):
        definition = json.loads(json.dumps(self._definition))
        definition["metrics"] = self.metrics
        definition["filters"] = self.parsed_filters(json_safe=True)
        return definition

    @property
    def slice_by(self):
        return self._definition.get("slice_by", [])

    @property
    def metrics(self):
        if "metric" in self._definition:
            metric_input = self._definition["metric"]
        else:
            metric_input = self._definition.get("metrics", [])
        return [metric_input] if isinstance(metric_input, str) else metric_input

    def _raw_filters(self):
        if self.filters is None:
            return []
        return self.filters

    def parsed_filters(self, json_safe=False):
        to_add = {"week_start_day": self.get_model().week_start_day, "timezone": self.project.timezone}
        return [f for raw in self._raw_filters() for f in Filter({**raw, **to_add}).filter_dict(json_safe)]

    def _error(self, element, error, extra: dict = {}):
        return self.dashboard._error(element, error, extra)

    def collect_errors(self):
        errors = []

        try:
            self.get_model()
        except (AccessDeniedOrDoesNotExistException, QueryError) as e:
            errors.append(
                self._error(self._definition.get("model"), str(e) + " in dashboard " + self.dashboard.name)
            )

        for field in self.metrics:
            if not self._function_executes(self.project.get_field, field):
                err_msg = f"Could not find field {field} referenced in dashboard {self.dashboard.name}"
                errors.append(self._error(self._definition.get("metrics"), err_msg))

        for field in self.slice_by:
            if not self._function_executes(self.project.get_field, field):
                err_msg = f"Could not find field {field} referenced in dashboard {self.dashboard.name}"
                errors.append(self._error(self._definition.get("slice_by"), err_msg))

        for f in self._raw_filters():
            if not self._function_executes(self.project.get_field, f.get("field")):
                err_msg = (
                    f"Could not find field {f.get('field')} referenced"
                    f" in a filter in dashboard {self.dashboard.name}"
                )
                errors.append(self._error(self._definition.get("filters"), err_msg))
        return errors

    @staticmethod
    def _function_executes(func, argument, **kwargs):
        try:
            func(argument, **kwargs)
            return True
        except Exception:
            return False


class Dashboard(MetricsLayerBase):
    def __init__(self, definition: dict, project) -> None:
        if "name" in definition:
            definition["name"] = self.normalize_name(definition["name"])

        if definition.get("layout") is None:
            definition["layout"] = DashboardLayouts.grid

        self.project: Project = project
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
                raise QueryError(f"Dashboard missing required key {k}")

    def to_dict(self):
        definition = json.loads(json.dumps(self._definition))
        definition["elements"] = [e.to_dict() for e in self.elements()]
        definition["filters"] = self.parsed_filters(json_safe=True)
        return definition

    def _error(self, element, error, extra: dict = {}):
        line, column = self.line_col(element)
        return {**extra, "dashboard_name": self.name, "message": error, "line": line, "column": column}

    def collect_errors(self):
        errors = []
        for f in self._raw_filters():
            try:
                self.project.get_field(f["field"])
            except Exception:
                err_msg = f"Could not find field {f['field']} referenced in a filter in dashboard {self.name}"
                errors.append(self._error(self._definition["filters"], err_msg))

        for element in self.elements():
            errors.extend(element.collect_errors())
        return errors

    def printable_attributes(self):
        to_print = ["name", "label", "description"]
        attributes = self.to_dict()
        attributes["type"] = "dashboard"
        return {key: attributes.get(key) for key in to_print if attributes.get(key) is not None}

    def _raw_filters(self):
        if self.filters is None:
            return []
        return self.filters

    def parsed_filters(self, json_safe=False):
        all_filters = []
        info = {"timezone": self.project.timezone}
        week_start_days = set(e.get_model().week_start_day for e in self.elements())
        if len(week_start_days) == 1:
            info["week_start_day"] = week_start_days.pop()
        else:
            info["week_start_day"] = None
        for f in self._raw_filters():
            clean_filters = Filter({**f, **info}).filter_dict(json_safe)
            for clean_filter in clean_filters:
                if clean_filter != {}:
                    all_filters.append(clean_filter)
        return all_filters

    def elements(self):
        elements = self._definition.get("elements", [])
        return [DashboardElement(e, dashboard=self, project=self.project) for e in elements]

    def _missing_filter_explore_error(self, filter_obj: dict):
        return (
            f"Argument 'explore' not found in the the filter {filter_obj} on dashboard "
            f"{self.name}. The 'explore' argument is required on filters for the whole dashboard."
        )
