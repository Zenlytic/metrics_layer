from typing import TYPE_CHECKING, Union

from metrics_layer.core.exceptions import QueryError

from .base import MetricsLayerBase

if TYPE_CHECKING:
    from metrics_layer.core.model.project import Project


class Set(MetricsLayerBase):
    valid_properties = ["name", "fields", "view_name"]

    def __init__(self, definition: dict, project) -> None:
        self.validate(definition)

        self.project: Project = project
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "fields"]
        for k in required_keys:
            if k not in definition:
                raise QueryError(f"Set missing required key {k}")

    def _error(self, element, error, extra: dict = {}):
        line, column = self.line_col(element)
        error = {**extra, "message": error, "line": line, "column": column}
        if self.view_name:
            error["view_name"] = self.view_name
        return error

    def collect_errors(self):
        errors = []
        if not self.valid_name(self.name):
            errors.append(self._error(self.name, self.name_error("set", self.name)))

        if "view_name" in self._definition and self.valid_name(self.view_name):
            try:
                self.project.get_view(self.view_name)
            except Exception as e:
                errors.append(self._error(self.view_name, f"In the Set {self.name} " + str(e)))

        if not isinstance(self.fields, list):
            errors.append(
                self._error(
                    self._definition["fields"],
                    f"The fields property, {self.fields} must be a list in the Set {self.name}",
                )
            )
        else:
            try:
                field_names = self.field_names()
            except Exception:
                # These errors are handled in the view in which the set is defined
                field_names = []

            for field in field_names:
                try:
                    self.project.get_field(field)
                except Exception as e:
                    errors.append(
                        self._error(self._definition["fields"], f"In the Set {self.name} " + str(e))
                    )

        errors.extend(
            self.invalid_property_error(
                self._definition, self.valid_properties, "set", self.name, error_func=self._error
            )
        )
        return errors

    def field_names(self):
        all_field_names, names_to_exclude = [], []
        for field_name in self.fields:
            # This means we're subtracting a set from the result
            if "*" in field_name and "-" in field_name:
                clean_name = field_name.replace("*", "").replace("-", "")
                fields_to_remove = self._internal_get_fields_from_set(clean_name)
                names_to_exclude.extend(fields_to_remove)

            # This means we're expanding a set into this set
            elif "*" in field_name:
                clean_name = field_name.replace("*", "")
                fields_to_add = self._internal_get_fields_from_set(clean_name)
                all_field_names.extend(fields_to_add)

            # This means we're removing a field from the result
            elif "-" in field_name:
                _, view_name, field_name = self.field_name_parts(field_name.replace("-", ""))
                view_name = self._get_view_name(view_name, field_name)
                names_to_exclude.append(f"{view_name}.{field_name}")

            # This is just a field that we're adding to the result
            else:
                _, view_name, field_name = self.field_name_parts(field_name)
                view_name = self._get_view_name(view_name, field_name)
                all_field_names.append(f"{view_name}.{field_name}")

        # Perform exclusion
        result_field_names = set(f for f in all_field_names if f not in names_to_exclude)

        return sorted(list(result_field_names), key=all_field_names.index)

    def _internal_get_fields_from_set(self, set_name: str):
        if set_name == "ALL_FIELDS":
            all_fields = self.project.fields(
                view_name=self.view_name, show_hidden=True, expand_dimension_groups=True
            )
            return [f"{f.view.name}.{f.alias()}" for f in all_fields]

        _, view_name, set_name = self.field_name_parts(set_name)
        _set = self.project.get_set(set_name, view_name=view_name)
        if _set is None:
            print(f"WARNING: Could not find set with name {set_name}, disregarding those fields")
            return []
        return _set.field_names()

    def _get_view_name(self, view_name: Union[None, str], field_name: str):
        if view_name:
            return view_name
        elif view_name is None and self.view_name:
            return self.view_name
        else:
            raise QueryError(f"Cannot find a valid view name for the field {field_name} in set {self.name}")
