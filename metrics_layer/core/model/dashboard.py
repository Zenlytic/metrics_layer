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


class DashboardElement(MetricsLayerBase):
    def __init__(self, definition: dict, dashboard, project) -> None:
        self.project: Project = project
        self.dashboard = dashboard
        self.validate(definition)
        super().__init__(definition)

    @property
    def valid_properties(self):
        # model is a valid model
        # title is a string
        # question_type is one of explore, funnel, analyze_baskets, explain_change
        # type is question or markdown
        # time_period is string (valid field filter)
        # filters (standard filters)
        # size one of full, half, quarter
        shared_properties = ["model", "title", "question_type", "type", "time_period", "filters", "size"]
        if self._definition.get("question_type", "explore") == "explore":
            # metrics is a list of strings
            # metric (deprecated) is a single string
            # slice_by is a list of strings
            # pivot_by is a list of strings
            # force_table: bool
            # row_limit int
            # show_annotations: bool
            # show_totals: bool
            # table_calculations: list of dict with: title: "Practitioner retention pct" formula: "[transaction_lines.number_of_patient_direct_practices] / cell_value([transaction_lines.number_of_patient_direct_practices], 1)" format: percent_1
            # compare_time_period: str
            # pivot_by_time_period: bool
            question_properties = [
                "metrics",
                "metric",
                "slice_by",
                "pivot_by",
                "force_table",
                "row_limit",
                "show_annotations",
                "show_totals",
                "table_calculations",
                "compare_time_period",
                "pivot_by_time_period",
            ]
        elif self._definition.get("question_type") == "explain_change":
            # metric is a string
            # compare_time_period is a string
            question_properties = ["metric", "compare_time_period"]
        elif self._definition.get("question_type") == "analyze_baskets":
            # metric is a string
            # category is a string reference to a field
            # combinations string, one of 1_&_2, 1, 2
            question_properties = ["metric", "category", "combinations"]
        elif self._definition.get("question_type") == "funnel":
            # metric is a string
            # steps: list of lists of filters
            # within dict with value (int) and unit one of FilterInterval type
            # slice_by list of strings max length 1
            question_properties = ["metric", "steps", "within", "slice_by"]
        else:
            raise QueryError(f"Invalid question type {self._definition.get('question_type')}")
        return shared_properties + question_properties

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

    def collect_errors(self):
        errors = []

        try:
            self.get_model()
        except (AccessDeniedOrDoesNotExistException, QueryError) as e:
            errors.append(str(e) + " in dashboard " + self.dashboard.name)

        for field in self.metrics + self.slice_by:
            if not self._function_executes(self.project.get_field, field):
                err_msg = f"Could not find field {field} referenced in dashboard {self.dashboard.name}"
                errors.append(err_msg)

        for f in self._raw_filters():
            if not self._function_executes(self.project.get_field, f["field"]):
                err_msg = (
                    f"Could not find field {f['field']} referenced"
                    f" in a filter in dashboard {self.dashboard.name}"
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
    valid_properties = ["name", "label", "description", "filters", "elements"]

    def __init__(self, definition: dict, project) -> None:
        if definition.get("name") is not None:
            definition["name"] = definition["name"].lower()

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

    def collect_errors(self):
        errors = []
        for f in self._raw_filters():
            try:
                self.project.get_field(f["field"])
            except Exception:
                err_msg = f"Could not find field {f['field']} referenced in a filter in dashboard {self.name}"
                errors.append(err_msg)

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
