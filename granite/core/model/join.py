from copy import deepcopy

from .base import GraniteBase, SQLReplacement


class Join(GraniteBase, SQLReplacement):
    def __init__(self, definition: dict = {}, view=None, explore=None) -> None:
        if definition.get("from") is not None:
            definition["from_"] = definition["from"]
        if "sql_on" in definition:
            definition["replaced_sql_on"] = self.get_replaced_sql_on(definition["sql_on"])

        self.validate(definition)
        self.view = view
        self.explore = explore
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "relationship", "type"]

        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Join missing required key {k}")

        neither_join_keys = "sql_on" not in definition and "foreign_key" not in definition
        both_join_keys = "sql_on" in definition and "foreign_key" in definition

        if both_join_keys or neither_join_keys:
            raise ValueError(f"Incorrect join identifiers sql_on and foreign_key (must have exactly one)")

        super().__init__(definition)

    def is_valid(self):
        fields_to_replace = self.fields_to_replace(self.sql_on)

        # The join isn't valid if we can't find an existing view with that name
        for field in fields_to_replace:
            view_name, _ = field.split(".")
            view = self._get_view_internal(view_name)
            if view is None:
                return False
        return True

    def to_dict(self):
        output = {**self._definition}
        output["sql_on"] = self.get_replaced_sql_on(output["sql_on"])
        return output if output["sql_on"] is not None else {}

    def get_replaced_sql_on(self, sql: str):
        sql_on = deepcopy(sql)
        fields_to_replace = self.fields_to_replace(sql_on)

        for field in fields_to_replace:
            view_name, column_name = field.split(".")
            view = self._get_view_internal(view_name)

            if view is None:
                return

            table_name = view.name
            field_obj = self.project.get_field(column_name, view_name=table_name)

            if field_obj and table_name:
                sql_condition = field_obj.get_replaced_sql_query()
                replace_with = sql_condition
            elif table_name:
                replace_with = f"{table_name}.{column_name}"
            else:
                replace_with = column_name

            replace_text = "${" + field + "}"
            sql_on = sql_on.replace(replace_text, replace_with)

        return sql_on

    def _get_view_internal(self, view_name: str):
        if self.from_ is not None and view_name == self.from_:
            view = self.project.get_view(self.from_)
        elif view_name == self.explore_from:
            view = self.project.get_view(self.explore_from)
        else:
            view = self.project.get_view(view_name)
        return view
