from copy import deepcopy
from typing import List
import itertools

import networkx
from metrics_layer.core.model.base import AccessDeniedOrDoesNotExistException, MetricsLayerBase
from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.sql.query_errors import ParseError


class MetricsLayerDesign:
    """ """

    def __init__(self, no_group_by: bool, query_type: str, field_lookup: dict, model, project) -> None:
        self.no_group_by = no_group_by
        self.query_type = query_type
        self.field_lookup = field_lookup
        self.project = project
        self.model = model
        self._joins = None
        self._required_views = None

    def views(self) -> List[MetricsLayerBase]:
        return self.project.views(model=self.model)

    def joins(self) -> List[MetricsLayerBase]:
        if self._joins is None:
            required_views = self.required_views()

            self._join_subgraph = self.project.join_graph.subgraph(required_views)

            try:
                ordered_view_pairs = self.determine_join_order(required_views)
            except networkx.exception.NetworkXNoPath:
                raise AccessDeniedOrDoesNotExistException(
                    f"There was no join path between the views: {required_views}. "
                    "Check the identifiers on your views and make sure they are joinable.",
                    object_name=None,
                    object_type="view",
                )

            self._joins = self.project.join_graph.ordered_joins(ordered_view_pairs)

        return self._joins

    def determine_join_order(self, required_views: list):
        if len(required_views) == 1:
            # There are no joins so we return empty list
            return []
        elif len(required_views) == 2:
            path = self._shortest_path_between_two(required_views)
            tuples = [(source, target) for source, target in zip(path, path[1:])]
            return tuples
        top_path = self._shortest_path_between_two(required_views)
        # TODO we will need to improve this method
        path = list(networkx.bfs_tree(self._join_subgraph, top_path[0]))
        tuples = [(source, target) for source, target in zip(path, path[1:])]
        return tuples

    def _shortest_path_between_two(self, required_views: list):
        valid_path_and_weights = []
        for start, end in itertools.permutations(required_views, 2):
            short_path = networkx.shortest_path(self.project.join_graph.graph, start, end, weight="weight")
            path_weight = networkx.path_weight(self.project.join_graph.graph, short_path, "weight")
            valid_path_and_weights.append((short_path, path_weight))

        shortest_path = sorted(valid_path_and_weights, key=lambda x: (x[-1], "".join(x[0])))[0][0]
        return shortest_path

    def required_views(self):
        if self._required_views is None:

            _, access_filter_fields = self.get_access_filter()
            fields_in_query = list(self.field_lookup.values()) + access_filter_fields
            self._required_views = self._fields_to_unique_views(fields_in_query)
        return self._required_views

    @staticmethod
    def _fields_to_unique_views(field_list: list):
        return list(set([v for field in field_list for v in field.required_views()]))

    # def _find_needed_joins(self, view_name: str, joins_already_added: list):
    #     joins_to_add = []

    #     join_already_added = any(view_name == j.from_ for j in joins_already_added)
    #     if not join_already_added and view_name != self.explore.from_:
    #         join = self.explore.get_join(view_name, by_view_name=True)
    #         if join is None:
    #             raise ValueError(
    #                 f"Could not locate join from view {view_name} for explore {self.explore.name}"
    #             )
    #         joins_to_add.append(join)
    #         for view_name in join.required_views():
    #             joins_to_add.extend(self._find_needed_joins(view_name, joins_already_added + [join]))
    #     return joins_to_add

    # def _sort_joins(self, joins_needed: list):
    #     if len(joins_needed) == 0:
    #         return []

    #     self._join_graph = networkx.DiGraph()
    #     for join in joins_needed:
    #         for view_name in join.required_views():
    #             if view_name != join.from_:
    #                 self._join_graph.add_edge(view_name, join.from_, relationship=join.relationship)

    #     self._ordered_join_names = list(networkx.topological_sort(self._join_graph))
    #     # Skip the first one because that's *always* the base of the explore
    #     return [self.explore.get_join(name, by_view_name=True) for name in self._ordered_join_names[1:]]

    def functional_pk(self):
        sorted_joins = self.joins()
        if len(sorted_joins) == 0:
            return self.get_view(self.base_view_name).primary_key
        elif any(j.relationship == "many_to_many" for j in sorted_joins):
            # There is no functional primary key if there is a many_to_many join
            return Definitions.does_not_exist
        elif all(j.relationship in {"many_to_one", "one_to_one"} for j in sorted_joins):
            # The functional primary key is the key to the base join is all joins are many_to_one
            return self.get_view(self.base_view_name).primary_key
        else:
            base_view = self.get_view(self.base_view_name)
            primary_key_view_name = self._derive_primary_key_view(base_view, sorted_joins)

            if primary_key_view_name == Definitions.does_not_exist:
                return Definitions.does_not_exist
            elif primary_key_view_name != base_view.name:
                primary_key_view = self.get_view(primary_key_view_name)
                return primary_key_view.primary_key
            return base_view.primary_key

    def _derive_primary_key_view(self, base_view, sorted_joins: list):
        # if the branch is from the base and many_to_one the base is the same
        # if the branch is from a many_to_one to the base and many_to_one or one_to_one it's the same
        # if the branch is from a many_to_one to the base and one_to_many it's now many_to_many

        # if the branch is from the base and one_to_one the base is the same
        # if the branch is from a one_to_one to the base and many_to_one or one_to_one it's the same
        # if the branch is from a one_to_one to the base and one_to_many the base is the new one

        # if the branch is from the base and one_to_many the base is the new one
        # if the branch is from a one_to_many to the base and many_to_one or one_to_one it's
        #   the one referenced in the one_to_many
        # if the branch is from a one_to_many to the base and one_to_many the base is now
        #   the newest one_to_many ref

        previous_join_type = None
        base_sequence = deepcopy([base_view.name])
        for i, j in enumerate(sorted_joins):
            previous_join_type = None if i == 0 else sorted_joins[i - 1].relationship
            if j.relationship == "many_to_many":
                return Definitions.does_not_exist

            if j.relationship == "one_to_many" and previous_join_type == "many_to_one":
                return Definitions.does_not_exist
            elif j.relationship == "one_to_many":
                base_sequence.append(j.join_view_name)
        primary_key = base_sequence[-1]
        return primary_key

    def get_view(self, name: str) -> MetricsLayerBase:
        try:
            return next(t for t in self.views() if t.name == name)
        except StopIteration:
            raise ParseError(f"View {name} not found in explore {self.explore.name}")

    def get_join(self, name: str) -> MetricsLayerBase:
        return next((j for j in self.joins() if j.name == name), None)

    def get_field(self, field_name: str) -> MetricsLayerBase:
        return self.project.get_field(field_name, model=self.model)

    def get_access_filter(self):
        views_in_request = self._fields_to_unique_views(list(self.field_lookup.values()))
        conditions, fields = [], []
        for view_name in views_in_request:
            view = self.get_view(view_name)
            if view.access_filters:
                for condition_set in view.access_filters:
                    field = self.project.get_field(condition_set["field"])
                    sql = field.sql_query(self.query_type)
                    user_attribute_value = condition_set["user_attribute"]

                    if self.project._user and self.project._user.get(user_attribute_value):
                        condition = f"{sql} = '{self.project._user[user_attribute_value]}'"
                        conditions.append(condition)
                        fields.append(field)

        if conditions and fields:
            return " and ".join(conditions), fields
        return None, []

    @property
    def base_view_name(self):
        joins = self.joins()
        if len(joins) > 0:
            return joins[0].base_view_name
        return self.required_views()[0]
