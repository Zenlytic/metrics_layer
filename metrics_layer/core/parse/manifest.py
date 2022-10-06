from metrics_layer.core.exceptions import QueryError


class Manifest:
    def __init__(self, definition: dict):
        self._definition = definition

    def exists(self):
        return self._definition is not None and self._definition != {}

    def get_model(self, model_name: str):
        return next(
            (
                v
                for v in self._definition["nodes"].values()
                if v["resource_type"] == "model" and v["alias"] == model_name
            ),
            None,
        )

    def models(self, schema: str = None, table: str = None):
        tables = []
        for v in self._definition["nodes"].values():
            is_model = v["resource_type"] == "model"

            # All tables in the whole database
            if is_model and schema is None and table is None:
                tables.append(self._node_to_table(v))
            # All tables in the schema with not table specified
            elif is_model and v["schema"] == schema and table is None:
                tables.append(self._node_to_table(v))
            # All tables matching the given table with not schema specified
            elif is_model and schema is None and v["alias"] == table:
                tables.append(self._node_to_table(v))
            # All tables matching the given table and schema specified
            elif is_model and v["schema"] == schema and v["alias"] == table:
                tables.append(self._node_to_table(v))

        return tables

    def _resolve_node(self, name: str):
        key = next((k for k in self._definition["nodes"].keys() if name == k.split(".")[-1]), None)
        if key is None:
            raise QueryError(
                f"Could not find the ref {name} in the co-located dbt project."
                " Please check the name in your dbt project."
            )
        return self._definition["nodes"][key]

    def resolve_name(self, name: str, schema_override=None):
        node = self._resolve_node(name)
        if schema_override is None:
            return self._node_to_table(node)
        return f"{schema_override}.{node['alias']}"

    @staticmethod
    def _node_to_table(node: dict):
        return f"{node['schema']}.{node['alias']}"
