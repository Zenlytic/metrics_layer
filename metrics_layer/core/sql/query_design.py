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
        return [self.project.get_view(name, explore=self.explore) for name in self.explore.view_names()]

    def joins(self) -> List[MetricsLayerBase]:
        if self._joins is None:
            fields_in_query = list(self.field_lookup.values())
            required_views = list(set([v for field in fields_in_query for v in field.required_views()]))
            if self.explore.always_join:
                required_views.extend(self.explore.always_join)

            joins_needed_for_query = []
            for view_name in reversed(sorted(required_views)):
                joins_needed_for_query.extend(self._find_needed_joins(view_name, joins_needed_for_query))
            sorted_joins = self._sort_joins(joins_needed_for_query)
            self._joins = sorted_joins
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

        G = networkx.DiGraph()
        for join in joins_needed:
            for view_name in join.required_views():
                G.add_edge(view_name, join.from_)
        ordered_names = list(networkx.bfs_tree(G, source=self.base_view_name))
        # Skip the first one because that's *always* the base of the explore
        return [self.explore.get_join(name, by_view_name=True) for name in ordered_names[1:]]

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
            join_path = self._derive_join_path(base_view, sorted_joins)
            print(join_path)
            raise
            return

    def _determine_resulting_pk(self, join_sequence: list, view_sequence: list):
        base_view = view_sequence[0]
        for join_type, next_view in zip(join_sequence, view_sequence[1:]):
            # TODO
            pass

    def _derive_join_path(self, base_view, sorted_joins: list):
        print(sorted_joins)

        G = networkx.DiGraph()
        for join in sorted_joins:
            for view_name in join.required_views():
                G.add_edge(view_name, join.from_, relationship=join.relationship)

        working_base = base_view.name
        for j in sorted_joins:
            print(j)
            print(G[j.from_])
            path = networkx.shortest_path(G, working_base, j.from_)
            print(path)
            r_sequence = []
            for from_, to_ in enumerate(range(1, len(path))):
                print(from_, to_)
                r = G[path[from_]][path[to_]]["relationship"]
                print(r)
                r_sequence.append(r)
            print(r_sequence)
            self._determine_resulting_pk(r_sequence, path)

        print(working_base)
        raise
        working_base_name = self.explore.name
        # one_to_many_connections_to_base = 0
        # join_connection_lookup = {}
        # for j in sorted_joins:
        #     if j.relationship == 'many_to_many':
        #         return Definitions.does_not_exist
        #     print(j.required_views())
        #     print(j.to_dict())
        #     required_joins = j.required_joins()
        #     connects_to_base = working_base_name in required_joins
        #     if connects_to_base:
        #         join_connection_lookup[j.name] = j.relationship
        #         if j.relationship == 'one_to_many':
        #             one_to_many_connections_to_base += 1
        #             working_base_name = j.name
        #     else:
        #         other_view_name = next((vn for vn in required_views if vn != j.from_))
        #         join_connection_lookup

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
