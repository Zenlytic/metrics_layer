from copy import deepcopy
from collections import Counter

from .base import MetricsLayerBase
from metrics_layer.core.exceptions import QueryError, AccessDeniedOrDoesNotExistException

SPECIAL_MAPPING_VALUES = {
    "date",
    "day_of_year",
    "week_of_year",
    "week",
    "month_of_year",
    "month",
    "quarter",
    "year",
}


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
        self.special_mapping_values = SPECIAL_MAPPING_VALUES
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

    @property
    def mappings(self):
        mappings = deepcopy(self._definition.get("mappings", {}))

        for date_mapping in self.special_mapping_values:
            if date_mapping in mappings:
                raise QueryError(
                    f"The mapping name {date_mapping} is a reserved name and cannot be used as a mapping name"
                )
            description = (
                f"The {date_mapping} associated with the metric or metrics you have "
                "in your query. When in doubt, use this to trend metrics over time."
            )
            all_canon_dates = [f.canon_date for f in self.project.fields()]
            unique_canon_dates = Counter(all_canon_dates).most_common()
            fields_mapped = []
            for d, _ in unique_canon_dates:
                canon_date_id = f"{d}_{date_mapping}"
                if d is not None and self.project.does_field_exist(canon_date_id):
                    fields_mapped.append(canon_date_id)
            # Includes all canon_dates in the project, sorted by number of occurrences in the project
            map_data = {
                "fields": fields_mapped,
                "group_label": "Dates",
                "description": description,
            }
            mappings[date_mapping] = map_data
        return mappings

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
        for mapping_name, mapped_values in self.mappings.items():
            special_mapping = mapping_name in self.special_mapping_values

            for mapped_from_field in mapped_values.get("fields", []):
                # If the user doesn't have access to a mapped field, we skip it but continue
                # to give them access to the original field which they *do* have access to
                try:
                    from_field = self.project.get_field(mapped_from_field)
                except AccessDeniedOrDoesNotExistException:
                    continue
                if dimensions_only and from_field.field_type == "measure":
                    continue

                from_join_hash = self.project.join_graph.join_graph_hash(from_field.view.name)
                # Handles the only allowed dimensions groups, the default ones for date, week, etc
                if from_field.field_type in {"dimension_group"} and not special_mapping:
                    raise QueryError(
                        "This mapping is invalid because it contains a dimension group. "
                        f"Mappings can only contain dimensions or measures. Mapping with {from_field.id()}"
                    )

                for mapped_to_field in mapped_values.get("fields", []):
                    if mapped_to_field != mapped_from_field:
                        try:
                            to_field = self.project.get_field(mapped_to_field)
                        except AccessDeniedOrDoesNotExistException:
                            continue
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
                            "is_canon_date_mapping": special_mapping,
                        }
                        dimension_mapping[mapped_from_field] = map_data

        return dimension_mapping
