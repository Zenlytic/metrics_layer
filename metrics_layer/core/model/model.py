from .base import MetricsLayerBase
from metrics_layer.core.exceptions import QueryError


class AccessGrant(MetricsLayerBase):
    def __init__(self, definition: dict = {}) -> None:

        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "user_attribute", "allowed_values"]
        for k in required_keys:
            if k not in definition:
                raise QueryError(f"Access Grant missing required key {k}")


class Model(MetricsLayerBase):
    def __init__(self, definition: dict = {}, project=None) -> None:

        self.project = project
        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "connection"]
        for k in required_keys:
            if k not in definition:
                raise QueryError(f"Model missing required key {k}")

    @property
    def access_grants(self):
        return self._definition.get("access_grants", [])

    def collect_errors(self):
        if not self.valid_name(self.name):
            return [self.name_error("model", self.name)]
        return []

    def printable_attributes(self):
        to_print = ["name", "type", "label", "group_label", "connection"]
        attributes = self.to_dict()
        return {key: attributes.get(key) for key in to_print if attributes.get(key) is not None}

    def get_mappings(self, dimensions_only: bool = False):
        if self.mappings is None:
            return {}

        dimension_mapping = {}
        for _, mapped_values in self.mappings.items():

            for mapped_from_field in mapped_values.get("fields", []):
                from_field = self.project.get_field(mapped_from_field)
                if dimensions_only and from_field.field_type == "measure":
                    continue

                from_join_hash = self.project.join_graph.join_graph_hash(from_field.view.name)
                if from_field.field_type in {"dimension_group"}:
                    raise QueryError(
                        "This mapping is invalid because it contains a dimension group. "
                        f"Mappings can only contain dimensions or measures. Mapping with {from_field.id()}"
                    )

                for mapped_to_field in mapped_values.get("fields", []):
                    if mapped_to_field != mapped_from_field:
                        to_field = self.project.get_field(mapped_to_field)
                        to_join_hash = self.project.join_graph.join_graph_hash(to_field.view.name)
                        if to_field.field_type != from_field.field_type:
                            raise QueryError(
                                f"This mapping is invalid because the mapped fields {mapped_from_field} "
                                f"and {mapped_to_field} are not the same type"
                            )
                        map_data = {
                            "field": mapped_to_field,
                            "field_type": to_field.field_type,
                            "from_join_hash": from_join_hash,
                            "to_join_hash": to_join_hash,
                        }
                        dimension_mapping[mapped_from_field] = map_data

        return dimension_mapping
