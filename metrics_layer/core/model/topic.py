from typing import TYPE_CHECKING

import networkx as nx

from metrics_layer.core.exceptions import (
    AccessDeniedOrDoesNotExistException,
    QueryError,
)

from .base import MetricsLayerBase
from .field import Field
from .join import Join
from .view import View

if TYPE_CHECKING:
    from metrics_layer.core.model.project import Project


class Topic(MetricsLayerBase):
    valid_properties = [
        "version",
        "type",
        "label",
        "base_view",
        "description",
        "zoe_description",
        "hidden",
        "required_access_grants",
        "access_filters",
        "always_filter",
        "views",
    ]
    internal_properties = ["_file_path"]

    def __init__(self, definition: dict, project) -> None:
        self.project: Project = project
        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["label", "base_view"]
        for k in required_keys:
            if k not in definition:
                name_str = ""
                if k != "label":
                    name_str = f" in the topic {definition.get('label')}"
                raise QueryError(f"Topic missing required key {k}{name_str}")

    def _error(self, element, error, extra: dict = {}):
        line, column = self.line_col(element)
        return {**extra, "model_name": self.name, "message": error, "line": line, "column": column}

    def collect_errors(self):
        errors = []

        if "label" in self._definition and not isinstance(self.label, str):
            errors.append(
                self._error(
                    self.label, f"The label property, {self.label} must be a string in the model {self.name}"
                )
            )

        if "base_view" in self._definition:
            if not isinstance(self.base_view, str):
                errors.append(
                    self._error(
                        self.base_view,
                        (
                            f"The base_view property, {self.base_view} must be a string"
                            f" in the topic {self.label}"
                        ),
                    )
                )

        if "description" in self._definition:
            if not isinstance(self.description, str):
                errors.append(
                    self._error(
                        self.description,
                        (
                            f"The description property, {self.description} must be a string in the topic"
                            f" {self.label}"
                        ),
                    )
                )

        if "zoe_description" in self._definition:
            if not isinstance(self.zoe_description, str):
                errors.append(
                    self._error(
                        self.zoe_description,
                        (
                            f"The zoe_description property, {self.zoe_description} must be a string in the"
                            f" topic {self.label}"
                        ),
                    )
                )

        if "hidden" in self._definition:
            if not isinstance(self.hidden, bool):
                errors.append(
                    self._error(
                        self.hidden,
                        f"The hidden property, {self.hidden} must be a boolean in the topic {self.label}",
                    )
                )

        if "always_filter" in self._definition and not isinstance(self.always_filter, list):
            errors.append(
                self._error(
                    self._definition["always_filter"],
                    (
                        f"The always_filter property, {self.always_filter} must be a list in the topic"
                        f" {self.label}"
                    ),
                )
            )
        elif "always_filter" in self._definition and isinstance(self.always_filter, list):
            for f in self.always_filter:
                if not isinstance(f, dict):
                    errors.append(
                        self._error(
                            self._definition["always_filter"],
                            f"Always filter {f} in topic {self.label} must be a dictionary",
                        )
                    )
                    continue

                if "field" in f and isinstance(f["field"], str) and "." not in f["field"]:
                    f["field"] = f"{self.name}.{f['field']}"
                errors.extend(
                    Field.collect_field_filter_errors(
                        f, self.project, "Always filter", "topic", self.label, error_func=self._error
                    )
                )

        if "access_filters" in self._definition and not isinstance(self.access_filters, list):
            access_filter_error = self._error(
                self._definition["access_filters"],
                (
                    f"The topic {self.label} has an access filter, {self.access_filters} that is incorrectly"
                    " specified as a when it should be a list, to specify it correctly check the"
                    " documentation for access filters at"
                    " https://docs.zenlytic.com/docs/data_modeling/access_grants#access-filters"
                ),
            )
            errors.append(access_filter_error)
        elif self.access_filters is not None and isinstance(self.access_filters, list):
            for f in self.access_filters:
                if not isinstance(f, dict):
                    errors.append(
                        self._error(
                            self._definition["access_filters"],
                            f"Access filter {f} in topic {self.label} must be a dictionary",
                        )
                    )
                    continue
                if "field" not in f:
                    errors.append(
                        self._error(
                            self._definition["access_filters"],
                            f"Access filter in topic {self.label} is missing the required property: 'field'",
                        )
                    )
                elif "field" in f:
                    try:
                        self.project.get_field(f["field"])
                    except AccessDeniedOrDoesNotExistException:
                        errors.append(
                            self._error(
                                f["field"],
                                (
                                    f"Access filter in topic {self.label} is referencing a field,"
                                    f" {f['field']} that does not exist"
                                ),
                            )
                        )
                if "user_attribute" not in f:
                    errors.append(
                        self._error(
                            self._definition["access_filters"],
                            (
                                f"Access filter in topic {self.label} is missing the required user_attribute"
                                " property"
                            ),
                        )
                    )
                elif "user_attribute" in f and not isinstance(f["user_attribute"], str):
                    errors.append(
                        self._error(
                            f["user_attribute"],
                            (
                                f"Access filter in topic {self.label} is referencing a user_attribute,"
                                f" {f['user_attribute']} that must be a string, but is not"
                            ),
                        )
                    )

        errors.extend(
            View.collect_required_access_grant_errors(
                self._definition,
                self.project,
                f"in topic {self.label}",
                "",
                error_func=self._error,
            )
        )

        # TODO tests for the 'views' property in the topic

        definition_to_check = {k: v for k, v in self._definition.items() if k not in self.internal_properties}
        errors.extend(
            self.invalid_property_error(
                definition_to_check, self.valid_properties, "model", self.name, error_func=self._error
            )
        )
        return errors

    def printable_attributes(self):
        to_print = ["type", "label", "description"]
        attributes = self.to_dict()
        return {key: attributes.get(key) for key in to_print if attributes.get(key) is not None}

    def get_view_overrides(self, view_name: str) -> list[dict]:
        return self.views.get(view_name, {})

    def query_validity_check(self, requested_views: list[str]) -> None:
        """
        Check if the requested views are valid for the topic.
        :param requested_views: list of view names that the user specifically asked for
        """
        available_views = [self.base_view] + list(self.views.keys())
        invalid_views = set(requested_views) - set(available_views)
        if invalid_views:
            invalid_views_str = ", ".join(sorted(invalid_views))
            raise QueryError(
                f"The following views are not included in the topic {self.label}: {invalid_views_str}\n\nYou"
                " can add them to the topic by adding the requested views to the topic."
            )

    def order_required_views(self, view_names: list[str]) -> list[str]:
        """Impute when other supporting joins are needed to complete a join
        :param view_names: list of view names that the user specifically asked for
        :return: list of view names in a valid topological order
        """
        self.query_validity_check(view_names)
        # Build a directed graph G where edges point from dependency -> dependent join
        G = nx.DiGraph()
        G.add_node(self.base_view)

        # Add edges for each join
        for view_name in self.views.keys():
            G.add_node(view_name)
            join = self.get_join(view_name)

            required_views = set(join.required_views()) - set([self.base_view, view_name])
            for required_view in required_views:
                G.add_edge(required_view, view_name)

        required_views = set()
        for r in view_names:
            required_views.add(r)
            required_views.update(nx.ancestors(G, r))  # all nodes that lead to r

        # Build a subgraph that only contains the required joins
        subG = G.subgraph(required_views).copy()

        sorted_views = list(nx.topological_sort(subG))
        return sorted_views

    def get_join(self, view_name: str) -> Join:
        # First check if the join exists in the topic as an override
        if view_name in self.views and "join" in self.views[view_name]:
            join_definition = {
                **self.views[view_name]["join"],
                "base_view_name": self.base_view,
                "join_view_name": view_name,
            }
            return Join(join_definition, self.project)
        try:
            join = self.project.join_graph.get_join(self.base_view, view_name)
            return join
        except KeyError:
            raise ValueError(f"Join not found between {self.base_view} and {view_name}")
