import networkx

from collections import defaultdict
from copy import deepcopy


class IdentifierTypes:
    primary = "primary"
    foreign = "foreign"
    join = "join"


class JoinGraph:
    def __init__(self, project) -> None:
        self.project = project
        self._join_preference = ["one_to_one", "many_to_one", "one_to_many", "many_to_many"]
        self._graph = None

    def views(self):
        return self.project.views()

    def subgraph(self, view_names: list):
        return self.graph.subgraph(view_names)

    @property
    def graph(self):
        self._graph = self.build()
        if self._graph is None:
            self._graph = self.build()
        return self._graph

    def build(self):
        graph = networkx.DiGraph()
        identifier_map = self._identifier_map()
        reference_map = self._reference_map()
        for view in self.project.views():
            graph.add_node(view.name)
            if view.name in reference_map:
                # Add all explicit "join" type references
                for join_view_name, join_identifier in reference_map[view.name].items():
                    graph.add_edge(view.name, join_view_name, relationship=join_identifier["relationship"])

            for identifier in view.identifiers:
                # Add all identifier matches across other views
                for join_view_name in identifier_map.get(identifier["name"], []):
                    if join_view_name != view.name:
                        join_view = self.project.get_view(join_view_name)
                        join_identifier = join_view.get_identifier(identifier["name"])
                        relationship = self._derive_relationship(identifier, join_identifier)
                        # Make sure the new join is preferable to the old one
                        if graph.has_edge(view.name, join_view_name):
                            existing = graph[view.name][join_view_name]["relationship"]
                            existing_score = self._join_preference.index(existing)
                            # Only if the new identifier gives us a more preferable join will we change
                            if existing_score > self._join_preference.index(relationship):
                                graph.add_edge(view.name, join_view_name, relationship=relationship)
                        else:
                            graph.add_edge(view.name, join_view_name, relationship=relationship)
        print(networkx.to_dict_of_dicts(graph))
        return graph

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
                    result[view.name][identifier["reference"]] = join_identifier
                    result[identifier["reference"]][view.name] = identifier
        return result

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
            raise ValueError(
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
