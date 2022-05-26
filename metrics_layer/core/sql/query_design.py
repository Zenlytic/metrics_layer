from copy import deepcopy
from typing import List
import itertools

import networkx
from metrics_layer.core.model.base import AccessDeniedOrDoesNotExistException, MetricsLayerBase
from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.sql.query_errors import ParseError


class MetricsLayerDesign:
    """ """

    def __init__(self, no_group_by: bool, query_type: str, field_lookup: dict, project) -> None:
        self.no_group_by = no_group_by
        self.query_type = query_type
        self.field_lookup = field_lookup
        self.project = project
        self._joins = None
        self._required_views = None

    def views(self) -> List[MetricsLayerBase]:
        return self.project.views()

    def joins(self) -> List[MetricsLayerBase]:
        if self._joins is None:
            required_views = self.required_views()

            # copied_views = deepcopy(required_views)
            # valid_paths = []
            # for start, end in itertools.permutations(copied_views, 2):
            #     short_path = networkx.shortest_path(
            #         self.project.join_graph.graph, start, end, weight="weight"
            #     )
            #     path_weight = networkx.path_weight(self.project.join_graph.graph, short_path, "weight")
            #     print(short_path)
            #     print(path_weight)
            #     print()

            # for v in copied_views:
            #     print(networkx.shortest_path(self.project.join_graph.graph, v, weight="weight"))

            #     for path in networkx.shortest_path(
            #         self.project.join_graph.graph, v, weight="weight"
            #     ).values():
            #         if all(v in path for v in copied_views):
            #             valid_paths.append(path)
            # print("VVV")
            # print(valid_paths)
            # if len(valid_paths) > 0:
            #     finalists = sorted(valid_paths, key=lambda x: len(x))
            #     print(finalists)
            #     if len(finalists[0]) > 1:
            #         srt = lambda x: networkx.path_weight(self.project.join_graph.graph, x, "weight")
            #         min_weight = min(srt(f) for f in finalists)
            #         equal = [f for f in finalists if srt(f) == min_weight]
            #         print(equal)
            #         required_views = sorted(equal, key=lambda x: "".join(x))[0]
            #         # required_views = sorted(finalists, key=srt)
            #     else:
            #         required_views = finalists[0]

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
            # print(networkx.to_dict_of_dicts(self._join_subgraph))
            # required_views
            # try:
            #     if len(required_views) == 1:
            #         ordered_views = required_views
            #     else:
            #         ordered_views = greedy_tsp(self._join_subgraph, weight="weight")[:-1]
            #         print("SELECT")
            #         print(ordered_views)
            #     # list(networkx.topological_sort(self._join_subgraph))
            # except Exception as e:
            #     print(e)
            #     try:
            #         print("yooo")
            #         print(
            #             networkx.is_simple_path(self._join_subgraph, ["orders", "customers", "order_lines"])
            #         )
            #         print(greedy_tsp(self._join_subgraph, weight="weight"))
            #         print(
            #             list(
            #                 networkx.bfs_tree(
            #                     self._join_subgraph,
            #                     "order_lines",  # sort_neighbors=lambda x: x["weight"]
            #                 )
            #             )
            #         )
            #     except Exception as ee:
            #         print(ee)
            #     ordered_views = required_views
            # print(ordered_views)
            # print(self.project.join_graph.ordered_joins(ordered_views))
            print(ordered_view_pairs)
            self._joins = self.project.join_graph.ordered_joins(ordered_view_pairs)

            # required_views = [view_name for j in self._joins for view_name in j.required_views()]
            # print(required_views)
            # print(networkx.to_dict_of_dicts(self._join_subgraph))

            # print(list(networkx.bfs_tree(subgraph, required_views[0])))
            # joins_needed_for_query = []
            # for view_name in reversed(sorted(required_views)):
            #     joins_needed_for_query.extend(self._find_needed_joins(view_name, joins_needed_for_query))
            # self._joins = self._sort_joins(joins_needed_for_query)
        return self._joins

    def determine_join_order(self, required_views: list):
        print(required_views)
        if len(required_views) == 1:
            # There are no joins so we return empty list
            return []
        elif len(required_views) == 2:
            path = self._shortest_path_between_two(required_views)
            tuples = [(source, target) for source, target in zip(path, path[1:])]
            return tuples
        top_path = self._shortest_path_between_two(required_views)
        print("helloo")
        print(networkx.bfs_tree(self.project.join_graph.graph, top_path[0]))
        # TODO we will need to improve this method
        path = list(networkx.bfs_tree(self._join_subgraph, top_path[0]))
        print(path)
        tuples = [(source, target) for source, target in zip(path, path[1:])]
        return tuples

        copied_views = deepcopy(required_views)
        accumulated_path = []
        while len(copied_views) > 1:
            top_path = self._shortest_path_between_two(copied_views)
            print(top_path)
            print(copied_views)
            next_node = top_path[0]
            print(next_node)
            copied_views.pop(copied_views.index(next_node))
            accumulated_path.append(top_path)
            # accumulated_path.append(next_node)

        # print(accumulated_path + copied_views)
        print(accumulated_path)
        return accumulated_path  # + copied_views

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
            required_views = list(set([v for field in fields_in_query for v in field.required_views()]))
            # if self.explore.always_join:
            #     required_views.extend(self.explore.always_join)

            self._required_views = required_views
        return self._required_views

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
                if view_name != join.from_:
                    self._join_graph.add_edge(view_name, join.from_, relationship=join.relationship)

        self._ordered_join_names = list(networkx.topological_sort(self._join_graph))
        # Skip the first one because that's *always* the base of the explore
        return [self.explore.get_join(name, by_view_name=True) for name in self._ordered_join_names[1:]]

    # @staticmethod
    # def chain_decomposition(graph, root):
    #     result = []
    #     for decomp in networkx.chain_decomposition(graph, root=root):
    #         if decomp[0][0] == root:
    #             result = []
    #             for edge in decomp:
    #                 result.append(edge[0])
    #     return result

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

    # def _pk_from_join_sequences(self, join_sequences: list):

    #     lengths, final_selections = [], []
    #     for sequence in join_sequences:
    #         lengths.append(len(sequence))
    #         final = sequence[-1]
    #         final_selections.append(final)

    #     # If all conclusions are the same than that's the right pk
    #     if len(set(final_selections)) == 1:
    #         return final_selections[0]

    #     # If there is disagreement in the final conclusions, we need to check for sub lists
    #     # E.g. if these are the join sequences:
    #     # [customers]
    #     # [customers, orders]
    #     # [customers, orders, order_lines]
    #     # The above is a pk of order_lines, because the differing conclusions are just sub oaths

    #     # e.g in this case they are not two sub-paths but actually different join paths
    #     # [customers, orders]
    #     # [customers, discounts]
    #     # The above is many_to_many
    #     longest_idx = lengths.index(max(lengths))
    #     longest_sequence = join_sequences[longest_idx]
    #     longest_final = final_selections[longest_idx]
    #     for sequence in join_sequences:
    #         if sequence[-1] != longest_final and not self._is_sublist(longest_sequence, sequence):
    #             return Definitions.does_not_exist
    #     print(longest_final)
    #     return longest_final

    # @staticmethod
    # def _is_sublist(main_list: list, sublist: list):
    #     n_contained_lists = len(main_list) - len(sublist) + 1
    #     return any(main_list[idx : idx + len(sublist)] == sublist for idx in range(n_contained_lists))

    def get_view(self, name: str) -> MetricsLayerBase:
        try:
            return next(t for t in self.views() if t.name == name)
        except StopIteration:
            raise ParseError(f"View {name} not found in explore {self.explore.name}")

    def get_join(self, name: str) -> MetricsLayerBase:
        return next((j for j in self.joins() if j.name == name), None)

    def get_field(self, field_name: str) -> MetricsLayerBase:
        return self.project.get_field(field_name)

    def get_access_filter(self):
        # TODO reimplement
        # if self.explore.access_filters:
        #     conditions, fields = [], []
        #     for condition_set in self.explore.access_filters:
        #         field = self.project.get_field(condition_set["field"], explore_name=self.explore.name)
        #         sql = field.sql_query(self.query_type)
        #         user_attribute_value = condition_set["user_attribute"]

        #         if self.project._user and self.project._user.get(user_attribute_value):
        #             condition = f"{sql} = '{self.project._user[user_attribute_value]}'"
        #             conditions.append(condition)
        #             fields.append(field)
        #     return " and ".join(conditions), fields
        return None, []

    @property
    def base_view_name(self):
        joins = self.joins()
        if len(joins) > 0:
            return joins[0].base_view_name
        return self.required_views()[0]
