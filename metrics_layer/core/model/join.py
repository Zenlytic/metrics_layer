from copy import deepcopy

from .base import MetricsLayerBase, SQLReplacement
from .field import Field


class Join(MetricsLayerBase, SQLReplacement):
    def __init__(self, definition: dict = {}, explore=None, project=None) -> None:
        self.project = project
        self.explore = explore
        if definition.get("from") is not None:
            definition["from_"] = definition["from"]
        elif definition.get("view_name") is not None:
            definition["from_"] = definition["view_name"]
        else:
            definition["from_"] = definition["name"]

        if "type" not in definition:
            definition["type"] = "left_outer"

        if "relationship" not in definition:
            definition["relationship"] = "many_to_one"

        self.validate(definition)
        super().__init__(definition)

    def replaced_sql_on(self, query_type: str):
        if self.sql_on:
            return self.get_replaced_sql_on(self.sql_on, query_type)
        return f"{self.explore.from_}.{self.foreign_key}={self.from_}.{self.foreign_key}"

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
        if self.sql_on:
            fields_to_replace = self.fields_to_replace(self.sql_on)

            # The join isn't valid if we can't find an existing view with that name
            for field in fields_to_replace:
                _, view_name, _ = Field.field_name_parts(field)
                if view_name not in self.explore.join_names():
                    err_msg = (
                        f"Could not find view {view_name} for join {self.name} in explore {self.explore.name}"
                    )
                    print(err_msg)
                    return False
            return True
        return self.foreign_key is not None

    def required_views(self):
        if not self.sql_on:
            return [self.explore.from_, self.from_]

        views = []
        for field in self.fields_to_replace(self.sql_on):
            _, join_name, _ = Field.field_name_parts(field)
            if join_name == self.explore.name:
                views.append(self.explore.from_)
            else:
                join = self.explore.get_join(join_name)
                views.append(join.from_)
        return list(set(views))

    def to_dict(self):
        output = {**self._definition}
        return output

    def get_replaced_sql_on(self, sql: str, query_type: str):
        sql_on = deepcopy(sql)
        fields_to_replace = self.fields_to_replace(sql_on)

        for field in fields_to_replace:
            _, join_name, column_name = Field.field_name_parts(field)
            if join_name == self.explore.name:
                view_name = self.explore.from_
            else:
                join = self.explore.get_join(join_name)
                view_name = join.from_
            view = self._get_view_internal(view_name)

            if view is None:
                return

            table_name = view.name
            field_obj = self.project.get_field(
                column_name, view_name=table_name, explore_name=self.explore.name
            )

            if field_obj and table_name:
                sql_condition = field_obj.sql_query(query_type)
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
        elif view_name == self.explore.from_:
            view = self.project.get_view(self.explore.from_)
        else:
            view = self.project.get_view(view_name)
        return view
