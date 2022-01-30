import re

from .base import MetricsLayerBase
from .field import Field
from .set import Set


class View(MetricsLayerBase):
    def __init__(self, definition: dict = {}, project=None) -> None:
        if "sets" not in definition:
            definition["sets"] = []
        self.project = project
        self.validate(definition)
        super().__init__(definition)

    @property
    def sql_table_name(self):
        if "sql_table_name" in self._definition:
            return self.resolve_sql_table_name(self._definition["sql_table_name"], self.project.looker_env)
        return

    def validate(self, definition: dict):
        required_keys = ["name", "fields"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"View missing required key {k}")

    def printable_attributes(self):
        to_print = ["name", "type", "label", "group_label", "sql_table_name", "number_of_fields"]
        attributes = self.to_dict()
        attributes["sql_table_name"] = self.sql_table_name
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

    def _all_fields(self):
        fields = []
        for f in self._definition.get("fields", []):
            field = Field(f, view=self)
            if self.project.can_access_field(field):
                fields.append(field)
        return fields

    def _valid_fields(self, expand_dimension_groups: bool):
        if expand_dimension_groups:
            fields = []
            for field in self._all_fields():
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
            fields = self._all_fields()

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
        if "-- if" in sql_table_name:
            return self._resolve_conditional_sql_table_name(sql_table_name, looker_env)
        if "ref(" in sql_table_name:
            return self._resolve_dbt_ref_sql_table_name(sql_table_name)
        return sql_table_name

    def _resolve_dbt_ref_sql_table_name(self, sql_table_name: str):
        ref_arguments = sql_table_name[sql_table_name.find("ref(") + 4 : sql_table_name.find(")")]
        ref_value = ref_arguments.replace("'", "")
        return self.project.resolve_dbt_ref(ref_value, self.name)

    @staticmethod
    def _resolve_conditional_sql_table_name(sql_table_name: str, looker_env: str):
        start_cond, end_cond = "-- if", "--"

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

    def list_sets(self):
        return [Set({**s, "view_name": self.name}, project=self.project) for s in self.sets]

    def get_set(self, set_name: str):
        return next((s for s in self.list_sets() if s.name == set_name), None)
