from copy import deepcopy
from typing import List
import functools
import itertools

import networkx
from metrics_layer.core.exceptions import JoinError
from metrics_layer.core.model.base import MetricsLayerBase
from metrics_layer.core.model.definitions import Definitions


class MetricsLayerDesign:
    """ """

    def __init__(self, no_group_by: bool, query_type: str, field_lookup: dict, model, project) -> None:
        self.no_group_by = no_group_by
        self.query_type = query_type
        self.field_lookup = field_lookup
        self.project = project
        self.model = model
        self.date_spine_cte_name = "date_spine"
        self.base_cte_name = "base"
        self._joins = None
        self._required_views = None

    def __hash__(self):
        return hash(self.project)

    @property
    def week_start_day(self):
        return self.model.week_start_day

    def views(self) -> List[MetricsLayerBase]:
        return self.project.views(model=self.model)

    @functools.lru_cache(maxsize=1)
    def joins(self) -> List[MetricsLayerBase]:
        required_views = self.required_views()

        self._join_subgraph = self.project.join_graph.subgraph(required_views)

        try:
            ordered_view_pairs = self.determine_join_order(required_views)
        except networkx.exception.NetworkXNoPath:
            raise JoinError(
                f"There was no join path between the views: {required_views}. "
                "Check the identifiers on your views and make sure they are joinable."
            )

        return self.project.join_graph.ordered_joins(ordered_view_pairs)

    def determine_join_order(self, required_views: list):
        if len(required_views) == 1:
            return []

        try:
            ordered_view_pairs = list(networkx.topological_sort(networkx.line_graph(self._join_subgraph)))
            ordered_view_pairs = self._clean_view_pairs(ordered_view_pairs)
            if len(required_views) > 1 and self._validate_join_path(ordered_view_pairs, required_views):
                raise networkx.exception.NetworkXUnfeasible
            return ordered_view_pairs

        except networkx.exception.NetworkXUnfeasible:

            if len(required_views) == 2:
                try:
                    path = self._shortest_path_between_two(required_views)
                    return [(source, target) for source, target in zip(path, path[1:])]
                except networkx.exception.NetworkXNoPath:
                    pass

            g = self.project.join_graph.graph
            raw_edges = networkx.line_graph(g).nodes
            sub_line_graph_nodes = networkx.line_graph(self._join_subgraph).nodes
            edges = [e for e in raw_edges if e[0] in required_views or e[1] in required_views]
            # Sorting puts the edges in the subgraph first, then sorts alphabetically
            for view_pair in sorted(edges, key=lambda x: (int(x in sub_line_graph_nodes) * -1, x)):
                try:
                    return self._greedy_build_join(g, view_pair, required_views)
                except ValueError:
                    pass

            raise networkx.exception.NetworkXNoPath

    def _validate_join_path(self, pairs: list, required_views: list):
        added_views = []
        for i, (v1, v2) in enumerate(pairs):
            if i == 0:
                added_views.extend([v1, v2])
            else:
                if v1 in added_views:
                    added_views.append(v2)
                else:
                    raise networkx.exception.NetworkXNoPath

        return any(v not in added_views for v in required_views)

    def _greedy_build_join(self, graph, starting_pair: tuple, required_views: list):
        _, paths = networkx.single_source_dijkstra(networkx.line_graph(graph), source=starting_pair)
        for pairs in paths.values():
            pairs = self._clean_view_pairs(pairs)
            unique_joined_views = set(v for p in pairs for v in p)

            missing_views = [v for v in required_views if v not in unique_joined_views]
            if len(missing_views) == 0:
                return pairs
            else:
                pairs = self._add_missing_views(missing_views, pairs, len(missing_views))
                return pairs

    def _add_missing_views(self, missing_views: str, pairs: list, missing_n: int):
        potential_anchors = [pairs[0][0]] + [p[-1] for p in pairs]
        still_missing = []
        for view_name in sorted(missing_views):
            to_test = []
            for potential_anchor in potential_anchors:
                to_test.append((potential_anchor, view_name))

            try:
                short_path = self._shortest_path_between_two(to_test, permute=False)
                path_ext = [(source, target) for source, target in zip(short_path, short_path[1:])]
                pairs.extend(path_ext)
            except networkx.exception.NetworkXNoPath:
                still_missing.append(view_name)

        if len(still_missing) == 0:
            return pairs
        elif len(still_missing) == missing_n:
            raise ValueError
        return self._add_missing_views(still_missing, pairs, len(missing_views))

    def _shortest_path_between_two(self, required_views: list, permute: bool = True):
        valid_path_and_weights = []
        # We need to do this because we don't know a priori which is the target and which is the finish
        if permute:
            view_pairs = itertools.permutations(required_views, 2)
        else:
            view_pairs = required_views
        for start, end in view_pairs:
            try:
                short_path = networkx.shortest_path(
                    self.project.join_graph.graph, start, end, weight="weight"
                )
                path_weight = networkx.path_weight(self.project.join_graph.graph, short_path, "weight")
                valid_path_and_weights.append((short_path, path_weight))
            except networkx.exception.NetworkXNoPath:
                pass

        if len(valid_path_and_weights) == 0:
            raise networkx.exception.NetworkXNoPath

        shortest_path = sorted(valid_path_and_weights, key=lambda x: (x[-1], "".join(x[0])))[0][0]
        return shortest_path

    def _clean_view_pairs(self, pairs: list):
        clean_pairs = []
        for i, pair in enumerate(pairs):
            included_in_query = [list(p) if j == 0 else [p[-1]] for j, p in enumerate(pairs[:i])]
            included_in_query = [v for sub_list in included_in_query for v in sub_list]
            duplicate_join = any(pair[-1] == p[-1] for p in pairs[:i])
            inverted_join = any(sorted(pair) == sorted(p) for p in pairs[:i])
            if not duplicate_join and not inverted_join and not pair[-1] in included_in_query:
                clean_pairs.append(pair)
        return clean_pairs

    def required_views(self):
        _, access_filter_fields = self.get_access_filter()
        fields_in_query = list(self.field_lookup.values()) + access_filter_fields
        return self._fields_to_unique_views(fields_in_query)

    @staticmethod
    def _fields_to_unique_views(field_list: list):
        return list(set([v for field in field_list for v in field.required_views()]))

    def deduplicate_fields(self, field_list: list):
        return self.project.deduplicate_fields(field_list)

    @functools.lru_cache(maxsize=1)
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
        return self.project.get_view(name, model=self.model)

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
