import functools
import itertools
from copy import deepcopy
from typing import List

import networkx

from metrics_layer.core.exceptions import JoinError
from metrics_layer.core.model.base import MetricsLayerBase
from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.model.filter import Filter
from metrics_layer.core.model.join import ZenlyticJoinRelationship
from metrics_layer.core.model.view import View


class MetricsLayerDesign:
    def __init__(
        self, no_group_by: bool, query_type: str, field_lookup: dict, model, project, topic=None
    ) -> None:
        self.no_group_by = no_group_by
        self.query_type = query_type
        self.field_lookup = field_lookup
        self.project = project
        self.model = model
        if topic is not None:
            self.topic = topic
        else:
            self.topic = None
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
        if self.topic:
            return self._joins_with_topic()
        else:
            return self._joins_with_no_topic()

    def _joins_with_topic(self) -> List[MetricsLayerBase]:
        base_view = self.topic.base_view
        required_views = self.topic.order_required_views(self.required_views())
        joins = []
        for view_name in required_views:
            if view_name != base_view:
                join = self.topic.get_join(view_name)
                # Case where the join exists in the join graph
                joins.append(join)
        return joins

    def _joins_with_no_topic(self) -> List[MetricsLayerBase]:
        required_views = self.required_views()

        self._join_subgraph = self.project.join_graph.subgraph(required_views)
        try:
            ordered_view_pairs = self.determine_join_order(required_views)
        except networkx.exception.NetworkXNoPath:
            raise JoinError(
                f"There was no join path between the views: {list(sorted(required_views))}. "
                "Check the identifiers on your views and make sure they are joinable."
            )

        return self.project.join_graph.ordered_joins(ordered_view_pairs)

    def view_symmetric_aggregate(self, view_name: str):
        sorted_joins = self.joins()

        # First, identify views that are directly involved in fan-out relationships
        one_to_many_joins = [
            join for join in sorted_joins if join.relationship == ZenlyticJoinRelationship.one_to_many
        ]
        many_to_many_joins = [
            join for join in sorted_joins if join.relationship == ZenlyticJoinRelationship.many_to_many
        ]

        # If there are more than one fan out join, we need symmetric
        # aggregates because the table is really fanned out, and
        # all views need to use the symmetric aggregate
        if len(one_to_many_joins) > 1 or len(many_to_many_joins) > 0:
            return Definitions.does_not_exist

        # If there are no fan out joins, the only case
        # that needs a symmetric aggregate is a many_to_one join
        elif len(one_to_many_joins) == 0 and len(many_to_many_joins) == 0:
            for join in sorted_joins:
                if (
                    join.relationship == ZenlyticJoinRelationship.many_to_one
                    and join.join_view_name == view_name
                ):
                    return Definitions.does_not_exist
            return None
        # If there is one fan out join, we need to check if the view is fanned out
        # If it is, we need to use the symmetric aggregate
        elif len(one_to_many_joins) == 1 and len(many_to_many_joins) == 0:
            for join in sorted_joins:
                if (
                    join.relationship not in {ZenlyticJoinRelationship.one_to_many}
                    and join.join_view_name == view_name
                ) or view_name == self.base_view_name:
                    return Definitions.does_not_exist
            return None
        else:
            raise ValueError("This state should not be possible")

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
            bridge_views = self._bridge_views(required_views)
            sub_line_graph_nodes = networkx.line_graph(self._join_subgraph).nodes
            edges = [e for e in raw_edges if e[0] in required_views or e[1] in required_views]
            sorted_edges = sorted(
                edges,
                key=lambda x: (
                    int(any(i in bridge_views for i in x)) * -1,
                    int(x in sub_line_graph_nodes) * -1,
                    x,
                ),
            )
            # Sorting puts the bridge views first, edges in the subgraph next, then sorts alphabetically
            for view_pair in sorted_edges:
                try:
                    return self._greedy_build_join(g, view_pair, required_views)
                except ValueError:
                    pass

            raise networkx.exception.NetworkXNoPath

    def _bridge_views(self, required_views: list):
        bridge_views = []
        for k, v in self.project.join_graph.composite_keys.items():
            if any(r in k for r in required_views):
                bridge_views.append(v)
        return bridge_views

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

    def _add_missing_views(self, missing_views: list, pairs: list, missing_n: int):
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
        elif any(j.relationship == ZenlyticJoinRelationship.many_to_many for j in sorted_joins):
            # There is no functional primary key if there is a many_to_many join
            return Definitions.does_not_exist
        elif all(
            j.relationship in {ZenlyticJoinRelationship.many_to_one, ZenlyticJoinRelationship.one_to_one}
            for j in sorted_joins
        ):
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
            if j.relationship == ZenlyticJoinRelationship.many_to_many:
                return Definitions.does_not_exist

            if (
                j.relationship == ZenlyticJoinRelationship.one_to_many
                and previous_join_type == ZenlyticJoinRelationship.many_to_one
            ):
                return Definitions.does_not_exist
            elif j.relationship == ZenlyticJoinRelationship.one_to_many:
                base_sequence.append(j.join_view_name)
        primary_key = base_sequence[-1]
        return primary_key

    def get_view(self, name: str) -> View:
        return self.project.get_view(name, model=self.model)

    def get_join(self, name: str) -> MetricsLayerBase:
        return next((j for j in self.joins() if j.name == name), None)

    def get_field(self, field_name: str) -> MetricsLayerBase:
        return self.project.get_field(field_name, model=self.model)

    def get_access_filter(self):
        views_in_request = self._fields_to_unique_views(list(self.field_lookup.values()))
        conditions, fields = [], []

        topic_assigned_user_attributes = set([])
        if self.topic and self.topic.access_filters:
            for condition_set in self.topic.access_filters:
                added_conditions, added_fields = self._process_access_filter_condition_set(condition_set)
                conditions.extend(added_conditions)
                fields.extend(added_fields)
                topic_assigned_user_attributes.add(condition_set["user_attribute"])

        for view_name in views_in_request:
            view = self.get_view(view_name)
            if view.access_filters:
                if self.topic:
                    topic_level_view_overrides = self.topic.get_view_overrides(view_name)
                    let_topic_override_view_access_filters = topic_level_view_overrides.get(
                        "override_access_filters", False
                    )
                else:
                    let_topic_override_view_access_filters = False
                for condition_set in view.access_filters:
                    skip_access_filter_assignment = (
                        let_topic_override_view_access_filters
                        and condition_set["user_attribute"] in topic_assigned_user_attributes
                    )

                    if not skip_access_filter_assignment:
                        added_conditions, added_fields = self._process_access_filter_condition_set(
                            condition_set
                        )
                        conditions.extend(added_conditions)
                        fields.extend(added_fields)

        if conditions and fields:
            return " and ".join(conditions), fields
        return None, []

    def _process_access_filter_condition_set(self, condition_set: dict):
        conditions, fields = [], []
        field = self.project.get_field(condition_set["field"])
        field_sql = field.sql_query(self.query_type)
        user_attribute_value = condition_set["user_attribute"]
        if self.project._user and self.project._user.get(user_attribute_value):
            f = Filter(
                {
                    "field": condition_set["field"],
                    "value": self.project._user[user_attribute_value],
                }
            )
            fields.append(field)
            for filter_dict in f.filter_dict():
                filter_sql = Filter.sql_query(
                    field_sql, filter_dict["expression"], filter_dict["value"], field.type
                )
                conditions.append(str(filter_sql))
        return conditions, fields

    @property
    def base_view_name(self):
        joins = self.joins()
        if len(joins) > 0:
            return joins[0].base_view_name
        return self.required_views()[0]
