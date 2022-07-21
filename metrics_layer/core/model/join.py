from copy import deepcopy

from metrics_layer.core.exceptions import AccessDeniedOrDoesNotExistException, QueryError
from .base import MetricsLayerBase, SQLReplacement
from .field import Field
from .set import Set


class Join(MetricsLayerBase, SQLReplacement):
    def __init__(self, definition: dict = {}, project=None) -> None:
        self.project = project
        if "type" not in definition:
            definition["type"] = "left_outer"
        if "relationship" not in definition:
            definition["relationship"] = "many_to_one"

        self.validate(definition)
        super().__init__(definition)

    def replaced_sql_on(self, query_type: str):
        return self.get_replaced_sql_on(self.sql_on, query_type)

    @property
    def name(self):
        return f"join between {self.base_view_name} and {self.join_view_name}"

    def validate(self, definition: dict):
        required_keys = ["base_view_name", "join_view_name", "relationship", "type"]

        for k in required_keys:
            if k not in definition:
                base_view_name, join_view_name = None, None
                if "base_view_name" in definition:
                    base_view_name = definition["base_view_name"]
                if "join_view_name" in definition:
                    join_view_name = definition["join_view_name"]
                raise QueryError(
                    f"Join missing required key {k} in join with view {base_view_name} and {join_view_name}"
                )

        has_sql_on = "sql_on" in definition
        has_fk = "foreign_key" in definition
        has_sql = "sql" in definition
        # Cross join won't need either sql_on or foreign key set
        is_cross_join = definition["type"] == "cross"
        all_join_arguments = [has_sql_on, has_fk, has_sql, is_cross_join]
        no_join_keys = all(not c for c in all_join_arguments)
        multiple_join_keys = sum(all_join_arguments) > 1

        if no_join_keys:
            raise QueryError(
                f"No join arguments found in join {definition['base_view_name']}, "
                "please pass sql_on or foreign_key"
            )

        if multiple_join_keys:
            raise QueryError(
                f"Multiple join arguments found in join {definition['base_view_name']}"
                ", please pass only one of: sql_on, foreign_key"
            )

    def collect_errors(self):
        errors = []
        if self.foreign_key:
            for view_name in [self.base_view_name, self.join_view_name]:
                try:
                    self.project.get_field(
                        self.foreign_key,
                        view_name=view_name,
                        show_excluded=True,
                    )
                except Exception:
                    errors.append(
                        f"Could not find field {self.foreign_key} in {self.name} "
                        f"referencing view {view_name}"
                    )
            return errors

        if self.sql_on:
            fields_to_replace = self.fields_to_replace(self.sql_on)

            for field in fields_to_replace:
                _, view_name, column_name = Field.field_name_parts(field)
                try:
                    view = self.project.get_view(view_name)
                except Exception:
                    err_msg = f"Could not find view {view_name} in {self.name}"
                    errors.append(err_msg)
                    continue

                try:
                    self.project.get_field(column_name, view_name=view.name)
                except Exception:
                    errors.append(
                        f"Could not find field {column_name} in {self.name} referencing view {view_name}"
                    )

        return errors

    def is_valid(self):
        if self.sql_on:
            fields_to_replace = self.fields_to_replace(self.sql_on)

            # The join isn't valid if we can't find an existing view with that name
            for field in fields_to_replace:
                _, view_name, _ = Field.field_name_parts(field)
                if view_name not in self.explore.join_names():
                    err_msg = f"Could not find view {view_name} for {self.name}"
                    print(err_msg)
                    return False
            return True
        is_valid = self.foreign_key is not None or self.type == "cross"
        return is_valid

    def required_views(self):
        if not self.sql_on:
            return [self.base_view_name, self.join_view_name]

        views = []
        for field in self.fields_to_replace(self.sql_on):
            _, view_name, _ = Field.field_name_parts(field)
            views.append(view_name)
        return list(set(views))

    def required_joins(self):
        if not self.sql_on:
            return [self.explore.name, self.name]

        joins = []
        for field in self.fields_to_replace(self.sql_on):
            _, join_name, _ = Field.field_name_parts(field)
            joins.append(join_name)
        return list(set(joins))

    def get_replaced_sql_on(self, sql: str, query_type: str):
        sql_on = deepcopy(sql)
        fields_to_replace = self.fields_to_replace(sql_on)

        for field in fields_to_replace:
            _, view_name, column_name = Field.field_name_parts(field)
            view = self.project.get_view(view_name)

            if view is None:
                return

            table_name = view.name
            field_obj = self.project.get_field(column_name, view_name=table_name)

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

    def field_names(self):
        # This function is for the explore `fields` parameter, to resolve all the sets into field names
        if self.fields is None:
            return self.fields

        set_definition = {
            "name": "NA",
            "fields": self.fields,
            "view_name": self.from_,
        }
        join_set = Set(set_definition, project=self.project)
        return join_set.field_names()

    def join_fields(self, show_hidden: bool, expand_dimension_groups: bool, show_excluded: bool):
        try:
            view = self.project.get_view(self.from_, explore=self.explore)
        except AccessDeniedOrDoesNotExistException:
            # If the user does not have access to the view, there are obviously no fields to show them
            return []
        fields = view.fields(show_hidden, expand_dimension_groups)
        join_field_names = self.field_names()
        if join_field_names and not show_excluded:
            return [f for f in fields if f.id() in join_field_names]
        return fields
