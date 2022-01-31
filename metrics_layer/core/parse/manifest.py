class Manifest:
    def __init__(self, definition: dict):
        self._definition = definition

    def exists(self):
        return self._definition is not None and self._definition != {}

    def _resolve_node(self, name: str):
        key = next((k for k in self._definition["nodes"].keys() if name == k.split(".")[-1]), None)
        if key is None:
            raise ValueError(
                f"Could not find the ref {name} in the co-located dbt project."
                " Please check the name in your dbt project."
            )
        return self._definition["nodes"][key]

    def resolve_name(self, name: str):
        node = self._resolve_node(name)
        # return f"{node['database']}.{node['schema']}.{node['alias']}"
        return f"{node['schema']}.{node['alias']}"
