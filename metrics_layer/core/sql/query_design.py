from copy import deepcopy
from typing import List

import networkx

from metrics_layer.core.model.base import MetricsLayerBase
from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.sql.query_errors import ParseError


class MetricsLayerDesign:
    """ """

    def __init__(self, no_group_by: bool, query_type: str, field_lookup: dict, explore, project) -> None:
        self.no_group_by = no_group_by
        self.query_type = query_type
        self.field_lookup = field_lookup
        self.explore = explore
        self.project = project
        self._joins = None

    def views(self) -> List[MetricsLayerBase]:
        return self.project.views(explore_name=self.explore.name)

    def joins(self) -> List[MetricsLayerBase]:
        if self._joins is None:
            _, access_filter_fields = self.get_access_filter()
            fields_in_query = list(self.field_lookup.values()) + access_filter_fields
            required_views = list(set([v for field in fields_in_query for v in field.required_views()]))
            if self.explore.always_join:
                required_views.extend(self.explore.always_join)

            joins_needed_for_query = []
            for view_name in reversed(sorted(required_views)):
                joins_needed_for_query.extend(self._find_needed_joins(view_name, joins_needed_for_query))
            self._joins = self._sort_joins(joins_needed_for_query)
        return self._joins

    def _find_needed_joins(self, view_name: str, joins_already_added: list):
        joins_to_add = []

        join_already_added = any(view_name == j.from_ for j in joins_already_added)
        if not join_already_added and view_name != self.explore.from_:
            join = self.explore.get_join(view_name, by_view_name=True)
            if join is None:
                raise ValueError(
                    f"Could not locate join from view {view_name} for explore {self.explore.name}"
                )
            joins_to_add.append(join)
            for view_name in join.required_views():
                joins_to_add.extend(self._find_needed_joins(view_name, joins_already_added + [join]))
        return joins_to_add

    def _sort_joins(self, joins_needed: list):
        if len(joins_needed) == 0:
            return []

        self._join_graph = networkx.DiGraph()
        for join in joins_needed:
            for view_name in join.required_views():
                self._join_graph.add_edge(view_name, join.from_, relationship=join.relationship)
        self._ordered_join_names = list(networkx.bfs_tree(self._join_graph, source=self.base_view_name))
        # Skip the first one because that's *always* the base of the explore
        return [self.explore.get_join(name, by_view_name=True) for name in self._ordered_join_names[1:]]

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

        working_base = base_view.name
        previous_join_type = None
        resolve_sequences = []
        for j in sorted_joins:
            if j.relationship == "many_to_many":
                return Definitions.does_not_exist

            # Since there can be many paths independent of each other, I have to check each
            # path to find what should be the base and comapre the results, if one path claims
            # to have a different base from the initial one then that path must the the only
            # one to make that claim (e.g. 2 one_to_many joins going from the same base
            # is a many_to_many resulting pk). This logic is in the
            path = networkx.shortest_path(self._join_graph, working_base, j.from_)

            base_sequence = deepcopy([base_view.name])
            previous_join_type = None
            for from_, to_ in enumerate(range(1, len(path))):
                relationship = self._join_graph[path[from_]][path[to_]]["relationship"]
                if relationship == "one_to_many" and previous_join_type == "many_to_one":
                    return Definitions.does_not_exist
                elif relationship == "one_to_many":
                    base_sequence.append(j.from_)
                previous_join_type = relationship
            resolve_sequences.append(base_sequence)
        primary_key = self._pk_from_join_sequences(resolve_sequences)
        return primary_key

    def _pk_from_join_sequences(self, join_sequences: list):

        lengths, final_selections = [], []
        for sequence in join_sequences:
            lengths.append(len(sequence))
            final = sequence[-1]
            final_selections.append(final)

        # If all conclusions are the same than that's the right pk
        if len(set(final_selections)) == 1:
            return final_selections[0]

        # If there is disagreement in the final conclusions, we need to check for sub lists
        # E.g. if these are the join sequences:
        # [customers]
        # [customers, orders]
        # [customers, orders, order_lines]
        # The above is a pk of order_lines, because the differing conclusions are just sub oaths

        # e.g in this case they are not two sub-paths but actually different join paths
        # [customers, orders]
        # [customers, discounts]
        # The above is many_to_many
        longest_idx = lengths.index(max(lengths))
        longest_sequence = join_sequences[longest_idx]
        longest_final = final_selections[longest_idx]
        for sequence in join_sequences:
            if sequence[-1] != longest_final and not self._is_sublist(longest_sequence, sequence):
                return Definitions.does_not_exist

        return longest_final

    @staticmethod
    def _is_sublist(main_list: list, sublist: list):
        n_contained_lists = len(main_list) - len(sublist) + 1
        return any(main_list[idx : idx + len(sublist)] == sublist for idx in range(n_contained_lists))

    def get_view(self, name: str) -> MetricsLayerBase:
        try:
            return next(t for t in self.views() if t.name == name)
        except StopIteration:
            raise ParseError(f"View {name} not found in explore {self.explore.name}")

    def get_join(self, name: str) -> MetricsLayerBase:
        return next((j for j in self.joins() if j.name == name), None)

    def get_field(self, field_name: str) -> MetricsLayerBase:
        return self.project.get_field(field_name, explore_name=self.explore.name)

    def get_access_filter(self):
        if self.explore.access_filters:
            conditions, fields = [], []
            for condition_set in self.explore.access_filters:
                field = self.project.get_field(condition_set["field"], explore_name=self.explore.name)
                sql = field.sql_query(self.query_type)
                user_attribute_value = condition_set["user_attribute"]

                if self.project._user and self.project._user.get(user_attribute_value):
                    condition = f"{sql} = '{self.project._user[user_attribute_value]}'"
                    conditions.append(condition)
                    fields.append(field)
            return " and ".join(conditions), fields
        return None, []

    @property
    def base_view_name(self):
        return self.explore.from_
