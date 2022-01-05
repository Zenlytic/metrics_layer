import re

from .base import MetricsLayerBase
from .field import Field


class View(MetricsLayerBase):
    def __init__(self, definition: dict = {}, project=None) -> None:
        if "sql_table_name" in definition:
            definition["sql_table_name"] = self.resolve_sql_table_name(
                definition["sql_table_name"], project.looker_env
            )

        if "sets" not in definition:
            definition["sets"] = []
        self.project = project
        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "fields"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"View missing required key {k}")

    def printable_attributes(self):
        to_print = ["name", "type", "label", "group_label", "sql_table_name", "number_of_fields"]
        attributes = self.to_dict()
        attributes["number_of_fields"] = f'{len(attributes.get("fields", []))}'
        return {key: attributes.get(key) for key in to_print if attributes.get(key) is not None}

    @property
    def primary_key(self):
        return next((f for f in self.fields() if f.primary_key == "yes"), None)

    def collect_errors(self):
        fields = self.fields(show_hidden=True)
        field_errors = []
        for field in fields:
            field_errors.extend(field.collect_errors())

        if self.primary_key is None:
            primary_key_error = (
                f"Warning: The view {self.name} does not have a primary key, "
                "specify one using the tag primary_key: yes"
            )
            field_errors += [primary_key_error]

        return field_errors

    def referenced_fields(self):
        fields = self.fields(show_hidden=True)
        result = []
        for field in fields:
            all_fields = [field] + field.get_referenced_sql_query(strings_only=False)
            result.extend(all_fields)
        return result

    def fields(self, show_hidden: bool = True, expand_dimension_groups: bool = False) -> list:
        all_fields = self._valid_fields(expand_dimension_groups=expand_dimension_groups)
        if show_hidden:
            return all_fields
        return [field for field in all_fields if field.hidden == "no" or not field.hidden]

    def _valid_fields(self, expand_dimension_groups: bool):
        if expand_dimension_groups:
            fields = []
            for f in self._definition.get("fields", []):
                field = Field(f, view=self)
                if field.field_type == "dimension_group" and field.timeframes:
                    for timeframe in field.timeframes:
                        if timeframe == "raw":
                            continue
                        field.dimension_group = timeframe
                        fields.append(field)
                elif field.field_type == "dimension_group" and field.intervals:
                    for interval in field.intervals:
                        field.dimension_group = f"{interval}s"
                        fields.append(field)
                else:
                    fields.append(field)
        else:
            fields = [Field(f, view=self) for f in self._definition.get("fields", [])]

        return fields

    def _field_name_to_remove(self, field_expr: str):
        # Skip the initial - sign
        field_clean_expr = field_expr[1:]
        if "." in field_clean_expr:
            view_name, field_name = field_clean_expr.split(".")
            if view_name == self.name:
                return field_name
            return None
        return field_clean_expr

    def resolve_sql_table_name(self, sql_table_name: str, looker_env: str):
        start_cond, end_cond = "-- if", "--"
        if start_cond in sql_table_name:
            # Find the condition that is chosen in the looker env
            conditions = re.findall(f"{start_cond}([^{end_cond}]*){end_cond}", sql_table_name)
            try:
                condition = next((cond for cond in conditions if cond.strip() == looker_env))
            except StopIteration:
                raise ValueError(
                    f"""Your sql_table_name: '{sql_table_name}' contains a conditional and
                    we could not match that to the conditional value you passed: {looker_env}"""
                )

            full_phrase = start_cond + condition + end_cond

            # Use regex to extract the value associated with the condition
            searchable_sql_table_name = sql_table_name.replace("\n", "")
            everything_between = f"{full_phrase}([^{end_cond}]*){end_cond}"
            everything_after = f"(?<={full_phrase}).*"
            result = re.search(everything_between, searchable_sql_table_name)
            if result:
                return result.group().replace(end_cond, "").strip()

            result = re.search(everything_after, searchable_sql_table_name)
            return result.group().strip()

        return sql_table_name

    def list_sets(self):
        return [Set({**s, "view_name": self.name}, project=self.project) for s in self.sets]

    def get_set(self, set_name: str):
        return next((s for s in self.list_sets() if s.name == set_name), None)


class Set(MetricsLayerBase):
    def __init__(self, definition: dict = {}, project=None) -> None:
        self.validate(definition)

        self.project = project
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "fields"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Set missing required key {k}")

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
                _, view_name, field_name = Field.field_name_parts(field_name.replace("-", ""))
                view_name = self._get_view_name(view_name, field_name)
                names_to_exclude.append(f"{view_name}.{field_name}")

            # This is just a field that we're adding to the result
            else:
                _, view_name, field_name = Field.field_name_parts(field_name)
                view_name = self._get_view_name(view_name, field_name)
                all_field_names.append(f"{view_name}.{field_name}")

        # Perform exclusion
        result_field_names = set(f for f in all_field_names if f not in names_to_exclude)

        return sorted(list(result_field_names), key=all_field_names.index)

    def _internal_get_fields_from_set(self, set_name: str):
        if set_name == "ALL_FIELDS":
            all_fields = self.project.fields(
                explore_name=self.explore_name,
                view_name=self.view_name,
                show_hidden=True,
                expand_dimension_groups=True,
                show_excluded=True,
            )
            return [f"{f.view.name}.{f.alias()}" for f in all_fields]

        _, view_name, set_name = Field.field_name_parts(set_name)
        _set = self.project.get_set(set_name, view_name=view_name)
        if _set is None:
            raise ValueError(f"Could not find set with name {set_name}")
        return _set.field_names()

    def _get_view_name(self, view_name: str, field_name: str):
        if view_name:
            return view_name
        elif view_name is None and self.view_name:
            return self.view_name
        else:
            raise ValueError(f"Cannot find a valid view name for the field {field_name} in set {self.name}")
