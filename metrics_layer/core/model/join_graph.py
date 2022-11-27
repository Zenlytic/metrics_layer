import networkx
from itertools import combinations

from metrics_layer.core.exceptions import QueryError
from collections import defaultdict
from copy import deepcopy
from .base import SQLReplacement
from .join import Join


class IdentifierTypes:
    primary = "primary"
    foreign = "foreign"
    join = "join"


class JoinGraph(SQLReplacement):
    def __init__(self, project) -> None:
        self.project = project
        self._join_preference = ["one_to_one", "many_to_one", "one_to_many", "many_to_many"]
        self._merged_result_graph = None
        self._graph = None
        self._field_memo = {}
        self._weak_graph_memo = {}
        self._strong_graph_memo = {}

    def subgraph(self, view_names: list):
        return self.graph.subgraph(view_names)

    @property
    def graph(self):
        if self._graph is None:
            self._graph = self.build()
        return self._graph

    def list_join_graphs(self):
        graph = self.project.join_graph.graph
        sorted_components = self._strongly_connected_components(graph)
        return [f"subquery_{i}" for i, _ in enumerate(sorted_components)]

    def join_graph_hash(self, view_name: str) -> str:
        if view_name not in self._strong_graph_memo:
            graph = self.project.join_graph.graph
            sorted_components = self._strongly_connected_components(graph)

            sorted_comps = enumerate(sorted_components)
            graph_hash = next((f"subquery_{i}" for i, comps in sorted_comps if view_name in comps), None)
            if graph_hash is None:
                raise QueryError(
                    f"View name {view_name} not found in any joinable part of your data model. "
                    "Please make sure this is the right name for the view."
                )

            self._strong_graph_memo[view_name] = graph_hash
        return self._strong_graph_memo[view_name]

    def weak_join_graph_hashes(self, view_name: str) -> list:
        if view_name not in self._weak_graph_memo:
            graph = self.project.join_graph.graph
            sorted_components = self._strongly_connected_components(graph)

            join_graph_hashes = []
            for i, components in enumerate(sorted_components):
                subgraph_nodes = self._subgraph_nodes_from_components(graph, components)
                if view_name in subgraph_nodes:
                    join_graph_hashes.append(f"subquery_{i}")

            self._weak_graph_memo[view_name] = join_graph_hashes
        return self._weak_graph_memo[view_name]

    def _strongly_connected_components(self, graph):
        components = networkx.strongly_connected_components(graph)
        sorted_components = sorted(components, key=len, reverse=True)  # Sort by largest component
        return sorted_components

    @staticmethod
    def _subgraph_nodes_from_components(graph, components):
        edges = networkx.edge_dfs(graph, source=components)
        all_edges = list(edges) + [list(components)]
        return list(set(node for edge in all_edges for node in edge))

    def ordered_joins(self, view_pairs: list):
        joins = []
        joined_views = []
        for base_view, join_view in view_pairs:
            join = self.get_join(base_view, join_view)
            if join_view not in joined_views:
                joins.append(join)
            joined_views.append(join_view)
        return joins

    def collect_errors(self):
        errors = []
        for join in self.joins():
            errors.extend(join.collect_errors())
        return errors

    def joins(self):
        joins = []
        for base_view_name, join_view_name in self.graph.edges():
            joins.append(self.get_join(base_view_name, join_view_name))
        return joins

    def get_join(self, base_view_name: str, join_view_name: str):
        join_info = self.graph[base_view_name][join_view_name]
        join_definition = {**join_info, "base_view_name": base_view_name, "join_view_name": join_view_name}
        return Join(join_definition, project=self.project)

    def build(self):
        graph = networkx.DiGraph()
        identifier_map = self._identifier_map()
        reference_map = self._reference_map()
        for view in self.project.views():
            graph.add_node(view.name)
            if view.name in reference_map:
                # Add all explicit "join" type references
                for join_view_name, join_identifier in reference_map[view.name].items():
                    graph.add_edge(view.name, join_view_name, **join_identifier)

            for identifier in view.identifiers:
                only_join = identifier.get("only_join", [])
                # Add all identifier matches across other views
                for join_view_name in identifier_map.get(identifier["name"], []):

                    if join_view_name != view.name and self._allowed_join(only_join, join_view_name):
                        join_view = self.project.get_view(join_view_name)
                        join_identifier = join_view.get_identifier(identifier["name"])
                        join_only_join = join_identifier.get("only_join", [])
                        if not self._allowed_join(join_only_join, view.name):
                            continue

                        join_info = self._identifier_to_join(
                            first_identifier=identifier,
                            first_view_name=view.name,
                            second_identifier=join_identifier,
                            second_view_name=join_view_name,
                        )
                        is_fanout = join_info["relationship"] in {"one_to_many", "many_to_many"}
                        if is_fanout and join_view_name not in identifier.get("allowed_fanouts", []):
                            continue

                        # Make sure the new join is preferable to the old one
                        if graph.has_edge(view.name, join_view_name):
                            existing = graph[view.name][join_view_name]["relationship"]
                            existing_score = self._join_preference.index(existing)
                            new_score = self._join_preference.index(join_info["relationship"])
                            # Only if the new identifier gives us a more preferable join will we change
                            if existing_score > new_score:
                                graph.add_edge(view.name, join_view_name, **join_info)
                        else:
                            graph.add_edge(view.name, join_view_name, **join_info)

        # print(networkx.to_dict_of_dicts(graph))
        return graph

    def merged_results_graph(self, model):
        if self._merged_result_graph is None:
            self._merged_result_graph = self._build_merged_results_graph(model)
        return self._merged_result_graph

    def _build_merged_results_graph(self, model):
        with_dates = [
            field
            for field in self.project.fields(model=model)
            if field.canon_date and field.field_type == "measure"
        ]
        mappings = model.get_mappings(dimensions_only=True)

        # Merged result shared date and field mapping
        graph = networkx.DiGraph()

        existing_root_nodes, join_group_hashes = self._add_canon_dates_to_merged_result(
            graph, with_dates, join_root="canon_date_core"
        )
        self._add_mappings_to_merged_result(
            graph, mappings, must_exist_in=join_group_hashes, root_nodes=existing_root_nodes
        )

        ordered_hashes = sorted(list(join_group_hashes))
        for join_group_hash_1, join_group_hash_2 in combinations(ordered_hashes, 2):
            join_root = join_group_hash_1 + "_" + join_group_hash_2

            pair = [join_group_hash_1, join_group_hash_2]
            existing_sub_root_nodes, _ = self._add_canon_dates_to_merged_result(
                graph, with_dates, join_root, use_condition=True, must_be_in=pair
            )

            # Add any fields that accessible via a join to both join_group_hash_1 and join_group_hash_2
            for view in self.project.views(model=model):
                join_hashes = self.project.join_graph.weak_join_graph_hashes(view.name)
                if all(join_hash in join_hashes for join_hash in pair):
                    view_fields = self.project.fields(
                        view_name=view.name, model=model, expand_dimension_groups=True
                    )
                    for field in view_fields:
                        for node in existing_sub_root_nodes:
                            graph.add_edge(node, field.id())

            self._add_mappings_to_merged_result(
                graph, mappings, must_exist_in=pair, root_nodes=existing_sub_root_nodes
            )

        return graph

    def _add_canon_dates_to_merged_result(
        self, graph, measures: list, join_root: str, use_condition: bool = False, must_be_in: list = []
    ):
        self._field_memo = {}
        existing_root_nodes, join_group_hashes = set(), set()
        for measure in measures:
            join_hash = self.project.join_graph.join_graph_hash(measure.view.name)
            join_group_hashes.add(join_hash)
            if not use_condition or (use_condition and join_hash in must_be_in):
                measure_id = measure.id()
                canon_date = self._get_field_with_memo(measure.canon_date, by_name=True)
                for timeframe in canon_date.timeframes:
                    canon_date.dimension_group = timeframe
                    root_node_name = join_root + "_" + timeframe
                    graph.add_edges_from([(root_node_name, canon_date.id()), (root_node_name, measure_id)])
                    existing_root_nodes.add(root_node_name)
        return sorted(list(existing_root_nodes)), join_group_hashes

    def _add_mappings_to_merged_result(self, graph, mappings: dict, must_exist_in: list, root_nodes: list):
        for from_field, mapping in mappings.items():
            to_field = mapping["field"]
            if mapping["from_join_hash"] in must_exist_in and mapping["to_join_hash"] in must_exist_in:
                from_ = self._get_field_with_memo(from_field).id()
                to_ = self._get_field_with_memo(to_field).id()
                for node in root_nodes:
                    graph.add_edges_from([(node, from_), (node, to_)])

    def _get_field_with_memo(self, field_name: str, by_name: bool = False):
        if field_name not in self._field_memo:
            if by_name:
                field = self.project.get_field_by_name(field_name)
            else:
                field = self.project.get_field(field_name)
            self._field_memo[field_name] = field
        else:
            field = self._field_memo[field_name]
        return field

    def _identifier_map(self):
        result = defaultdict(list)
        for view in self.project.views():
            for identifier in view.identifiers:
                if identifier["type"] != IdentifierTypes.join:
                    result[identifier["name"]].append(view.name)
        return result

    def _reference_map(self):
        result = defaultdict(dict)
        for view in self.project.views():
            for identifier in view.identifiers:
                if identifier["type"] == IdentifierTypes.join:
                    join_identifier = deepcopy(identifier)
                    join_identifier["relationship"] = self._invert_relationship(
                        join_identifier["relationship"]
                    )
                    # We need to invert the join here because this is the inverse
                    # direction of how the join was defined
                    result[view.name][identifier["reference"]] = self._verify_identifier_join(identifier)
                    result[identifier["reference"]][view.name] = self._verify_identifier_join(join_identifier)
        return result

    def _identifier_to_join(self, first_identifier, first_view_name, second_identifier, second_view_name):
        relationship = self._derive_relationship(first_identifier, second_identifier)
        join_type = "left_outer"
        first_clause = self._identifier_join_clause(first_identifier, first_view_name)
        second_clause = self._identifier_join_clause(second_identifier, second_view_name)
        sql_on = f"{first_clause}={second_clause}"
        weight = self._edge_weight(relationship)
        return {"relationship": relationship, "type": join_type, "sql_on": sql_on, "weight": weight}

    def _identifier_join_clause(self, identifier: dict, view_name: str):
        if "sql" in identifier:
            cleaned_sql = deepcopy(identifier["sql"])
            for field_name in self.fields_to_replace(identifier["sql"]):
                to_replace = "${" + field_name + "}"
                if field_name != "TABLE" and "." not in field_name:
                    cleaned_reference = "${" + f"{view_name}.{field_name}" + "}"
                    cleaned_sql = cleaned_sql.replace(to_replace, cleaned_reference)
            clause = cleaned_sql
        else:
            clause = "${" + f"{view_name}.{identifier['name']}" + "}"
        return clause

    def _verify_identifier_join(self, join: dict):
        clean_join = deepcopy(join)
        clean_join["type"] = join.get("type", "left_outer")
        clean_join["relationship"] = join.get("relationship", "many_to_one")
        clean_join["sql_on"] = join["sql_on"]
        clean_join["weight"] = self._edge_weight(clean_join["relationship"])
        return clean_join

    @staticmethod
    def _derive_relationship(identifier, join_identifier):
        base_type = identifier["type"]
        join_type = join_identifier["type"]
        if base_type == IdentifierTypes.foreign and join_type == IdentifierTypes.primary:
            return "many_to_one"
        elif base_type == IdentifierTypes.primary and join_type == IdentifierTypes.primary:
            return "one_to_one"
        elif base_type == IdentifierTypes.primary and join_type == IdentifierTypes.foreign:
            return "one_to_many"
        elif base_type == IdentifierTypes.foreign and join_type == IdentifierTypes.foreign:
            return "many_to_many"
        else:
            raise QueryError(
                "This join type cannot be determined from the identifier properties. "
                f"Make sure you've set the properties correctly. Base type: {base_type},"
                f" join type: {join_type}"
            )

    @staticmethod
    def _invert_relationship(relationship: str):
        mapping = {
            "many_to_one": "one_to_many",
            "one_to_many": "many_to_one",
            "many_to_many": "many_to_many",
            "one_to_one": "one_to_one",
        }
        return mapping[relationship]

    @staticmethod
    def _edge_weight(relationship: str):
        mapping = {
            "many_to_one": 2,
            "one_to_many": 3,
            "many_to_many": 4,
            "one_to_one": 1,
        }
        return mapping[relationship]

    @staticmethod
    def _allowed_join(only_join: list, view_name: str):
        return not only_join or (only_join and view_name in only_join)
