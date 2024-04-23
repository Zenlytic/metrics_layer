import json
from collections import Counter
from typing import TYPE_CHECKING

import pendulum

from metrics_layer.core.exceptions import (
    AccessDeniedOrDoesNotExistException,
    QueryError,
)

from .base import MetricsLayerBase
from .week_start_day_types import WeekStartDayTypes

if TYPE_CHECKING:
    from metrics_layer.core.model.project import Project

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
    valid_properties = ["name", "user_attribute", "allowed_values"]

    def __init__(self, definition: dict, model) -> None:
        self.model: Model = model
        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "user_attribute", "allowed_values"]
        for k in required_keys:
            if k not in definition:
                name_str = ""
                if k != "name":
                    name_str = f" {definition.get('name')}"
                raise QueryError(f"Access Grant{name_str} missing required key {k}")

    def _error(self, element, error):
        return self.model._error(element, error)

    def collect_errors(self):
        errors = []
        if not self.valid_name(self.name):
            errors.append(self._error(self.name, self.name_error("Access Grant", self.name)))

        if not isinstance(self.user_attribute, str):
            errors.append(
                self._error(
                    self.user_attribute,
                    (
                        f"The user_attribute property, {self.user_attribute} must be a string in the Access"
                        f" Grant {self.name}"
                    ),
                )
            )

        if not isinstance(self.allowed_values, list):
            errors.append(
                self._error(
                    self.allowed_values,
                    (
                        f"The allowed_values property, {self.allowed_values} must be a list in the Access"
                        f" Grant {self.name}"
                    ),
                )
            )
        elif all([not isinstance(value, str) for value in self.allowed_values]):
            errors.append(
                self._error(
                    self.allowed_values,
                    (
                        "All values in the allowed_values property must be strings in the Access Grant"
                        f" {self.name}"
                    ),
                )
            )

        errors.extend(
            self.invalid_property_error(
                self._definition, self.valid_properties, "access grant", self.name, error_func=self._error
            )
        )
        return errors


class Model(MetricsLayerBase):
    valid_properties = [
        "version",
        "type",
        "name",
        "label",
        "connection",
        "week_start_day",
        "timezone",
        "default_convert_tz",
        "access_grants",
        "mappings",
    ]
    internal_properties = ["_file_path"]

    def __init__(self, definition: dict, project) -> None:
        self.special_mapping_values = SPECIAL_MAPPING_VALUES
        self.project: Project = project
        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "connection"]
        for k in required_keys:
            if k not in definition:
                name_str = ""
                if k != "name":
                    name_str = f" in the model {definition.get('name')}"
                raise QueryError(f"Model missing required key {k}{name_str}")

    @property
    def access_grants(self):
        if "access_grants" in self._definition:
            if not isinstance(self._definition["access_grants"], list):
                raise QueryError(
                    f"The access_grants property, {self._definition['access_grants']} must be a list"
                )
            elif all([not isinstance(grant, dict) for grant in self._definition["access_grants"]]):
                raise QueryError(f"All access_grants in the access_grants property must be dictionaries")
            return self._definition["access_grants"]
        return []

    @property
    def mappings(self):
        mappings = json.loads(json.dumps(self._definition.get("mappings", {})))

        if not isinstance(mappings, dict):
            raise QueryError(f"The mappings property, {mappings} must be a dictionary")

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
            map_data = {"fields": fields_mapped, "group_label": "Dates", "description": description}
            mappings[date_mapping] = map_data
        return mappings

    def _error(self, element, error, extra: dict = {}):
        line, column = self.line_col(element)
        return {**extra, "model_name": self.name, "message": error, "line": line, "column": column}

    def collect_errors(self):
        errors = []
        if not self.valid_name(self.name):
            errors.append(self._error(self.name, self.name_error("model", self.name)))

        if not isinstance(self.connection, str):
            errors.append(
                self._error(
                    self.connection,
                    f"The connection property, {self.connection} must be a string in the model {self.name}",
                )
            )

        if "label" in self._definition and not isinstance(self.label, str):
            errors.append(
                self._error(
                    self.label, f"The label property, {self.label} must be a string in the model {self.name}"
                )
            )

        if "week_start_day" in self._definition:
            if str(self.week_start_day) not in WeekStartDayTypes.options:
                errors.append(
                    self._error(
                        self.week_start_day,
                        (
                            f"The week_start_day property, {self.week_start_day} must be one of"
                            f" {WeekStartDayTypes.options} in the model {self.name}"
                        ),
                    )
                )

        if "timezone" in self._definition:
            try:
                pendulum.timezone(str(self.timezone))
            except Exception:
                errors.append(
                    self._error(
                        self.timezone,
                        (
                            f"The timezone property, {self.timezone} must be a valid timezone in the model"
                            f" {self.name}. Valid timezones can be found at"
                            " https://en.wikipedia.org/wiki/List_of_tz_database_time_zones"
                        ),
                    )
                )

        if "default_convert_tz" in self._definition and not isinstance(self.default_convert_tz, bool):
            errors.append(
                self._error(
                    self.default_convert_tz,
                    (
                        f"The default_convert_tz property, {self.default_convert_tz} must be a boolean in the"
                        f" model {self.name}"
                    ),
                )
            )

        try:
            if self.access_grants:
                for access_grant in self.access_grants:
                    try:
                        grant = AccessGrant(access_grant, model=self)
                        errors.extend(grant.collect_errors())
                    except QueryError as e:
                        errors.append(self._error(access_grant, str(e) + f" in the model {self.name}"))
        except QueryError as e:
            errors.append(
                self._error(self._definition["access_grants"], str(e) + f" in the model {self.name}")
            )
        valid_mappings_properties = ["fields", "group_label", "description", "link", "label"]
        try:
            mappings = self.mappings
            for mapping_name, mapped_values in mappings.items():
                mapping_element = self._definition.get("mappings", {}).get(mapping_name)
                if not isinstance(mapping_name, str):
                    errors.append(
                        self._error(
                            mapping_element,
                            f"The mapping name, {mapping_name} must be a string in the model {self.name}",
                        )
                    )
                if not self.valid_name(mapping_name):
                    errors.append(
                        self._error(
                            mapping_element,
                            (
                                f"The mapping name, {mapping_name} is invalid. Please reference the naming"
                                " conventions (only letters, numbers, or underscores) in the model"
                                f" {self.name}"
                            ),
                        )
                    )

                if not isinstance(mapped_values, dict):
                    errors.append(
                        self._error(
                            mapping_element,
                            (
                                f"The mapping value, {mapped_values} must be a dictionary in the model"
                                f" {self.name}"
                            ),
                        )
                    )
                if isinstance(mapped_values, dict):
                    if "link" in mapped_values and not isinstance(mapped_values["link"], str):
                        errors.append(
                            self._error(
                                mapping_element,
                                (
                                    f"The link property, {mapped_values['link']} must be a string"
                                    f" in the mapping {mapping_name} in the model {self.name}"
                                ),
                            )
                        )

                    if "group_label" in mapped_values and not isinstance(mapped_values["group_label"], str):
                        errors.append(
                            self._error(
                                mapping_element,
                                (
                                    f"The group_label property, {mapped_values['group_label']} must be a"
                                    f" string in the mapping {mapping_name} in the model {self.name}"
                                ),
                            )
                        )
                    if "description" in mapped_values and not isinstance(mapped_values["description"], str):
                        errors.append(
                            self._error(
                                mapping_element,
                                (
                                    f"The description property, {mapped_values['description']} must be a"
                                    f" string in the mapping {mapping_name} in the model {self.name}"
                                ),
                            )
                        )
                    if not isinstance(mapped_values.get("fields"), list):
                        errors.append(
                            self._error(
                                mapping_element,
                                (
                                    f"The fields property, {mapped_values['fields']} must be a list"
                                    f" in the mapping {mapping_name} in the model {self.name}"
                                ),
                            )
                        )
                    elif isinstance(mapped_values.get("fields"), list):
                        for field_id in mapped_values["fields"]:
                            try:
                                self.project.get_field(field_id)
                            except AccessDeniedOrDoesNotExistException as e:
                                errors.append(
                                    self._error(
                                        mapping_element,
                                        f"In the mapping {mapping_name} in the model {self.name}, the "
                                        + str(e),
                                        {"field_name": field_id},
                                    )
                                )

                    errors.extend(
                        self.invalid_property_error(
                            mapped_values,
                            valid_mappings_properties,
                            "mapping",
                            mapping_name,
                            error_func=self._error,
                        )
                    )
        except QueryError as e:
            if "Field" in str(e) and "missing required key" in str(e):
                raise QueryError(str(e) + f" in the model {self.name}")
            errors.append(
                self._error(self._definition.get("mappings"), str(e) + f" in the model {self.name}")
            )

        definition_to_check = {k: v for k, v in self._definition.items() if k not in self.internal_properties}
        errors.extend(
            self.invalid_property_error(
                definition_to_check, self.valid_properties, "model", self.name, error_func=self._error
            )
        )
        return errors

    def printable_attributes(self):
        to_print = ["name", "type", "label", "connection"]
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
                        reference = {"field": mapped_to_field, "to_join_hash": to_join_hash}
                        # Create an object that contains the mapping from the field to the fields it
                        # maps to with all their metadata under the references array
                        if mapped_from_field in dimension_mapping:
                            dimension_mapping[mapped_from_field]["references"].append(reference)
                        else:
                            map_data = {
                                "references": [reference],
                                "field_type": from_field.field_type,
                                "from_join_hash": from_join_hash,
                                "is_canon_date_mapping": special_mapping,
                            }
                            dimension_mapping[mapped_from_field] = map_data

        return dimension_mapping
