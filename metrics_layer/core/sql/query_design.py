from typing import List

import networkx

from metrics_layer.core.model.base import MetricsLayerBase
from metrics_layer.core.sql.query_errors import ParseError


class MetricsLayerDesign:
    """ """

    def __init__(self, no_group_by: bool, query_type: str, field_lookup: dict, explore, project) -> None:
        self.no_group_by = no_group_by
        self.query_type = query_type
        self.field_lookup = field_lookup
        self.explore = explore
        self.project = project

    def views(self) -> List[MetricsLayerBase]:
        return [self.project.get_view(name, explore=self.explore) for name in self.explore.view_names()]

    def joins(self) -> List[MetricsLayerBase]:
        fields_in_query = list(self.field_lookup.values())
        required_views = list(set([v for field in fields_in_query for v in field.required_views()]))
        if self.explore.always_join:
            required_views.extend(self.explore.always_join)

        joins_needed_for_query = []
        for view_name in reversed(sorted(required_views)):
            joins_needed_for_query.extend(self._find_needed_joins(view_name, joins_needed_for_query))
        return self._sort_joins(joins_needed_for_query)

    def _find_needed_joins(self, view_name: str, joins_already_added: list):
        joins_to_add = []

        join_already_added = any(view_name == j.name for j in joins_already_added)
        if not join_already_added and view_name != self.explore.from_:
            join = self.explore.get_join(view_name)
            if join is None:
                raise ValueError(f"Could not locate join named {view_name} for explore {self.explore.name}")
            joins_to_add.append(join)
            for view_name in join.required_views():
                joins_to_add.extend(self._find_needed_joins(view_name, joins_already_added + [join]))
        return joins_to_add

    def _sort_joins(self, joins_needed: list):
        if len(joins_needed) == 0:
            return []

        G = networkx.DiGraph()
        for join in joins_needed:
            for view_name in join.required_views():
                G.add_edge(view_name, join.name)
        ordered_names = list(networkx.bfs_tree(G, source=self.base_view_name))
        # Skip the first one because that's *always* the base of the explore
        return [self.explore.get_join(name) for name in ordered_names[1:]]

    def get_view(self, name: str) -> MetricsLayerBase:
        try:
            return next(t for t in self.views() if t.name == name)
        except StopIteration:
            raise ParseError(f"View {name} not found in explore {self.explore.name}")

    def get_join(self, name: str) -> MetricsLayerBase:
        return next((j for j in self.joins() if j.name == name), None)

    def get_field(self, field_name: str, view_name: str = None) -> MetricsLayerBase:
        return self.project.get_field(field_name, view_name=view_name, explore_name=self.explore.name)

    @property
    def base_view_name(self):
        return self.explore.from_
