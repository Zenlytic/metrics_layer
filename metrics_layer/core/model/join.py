from copy import deepcopy

from .base import AccessDeniedOrDoesNotExistException, MetricsLayerBase, SQLReplacement
from .field import Field
from .set import Set


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
                join_name = None
                if "name" in definition:
                    join_name = definition["name"]
                raise ValueError(f"Join missing required key {k} in join {join_name}")

        has_sql_on = "sql_on" in definition
        has_fk = "foreign_key" in definition
        has_sql = "sql" in definition
        all_join_arguments = [has_sql_on, has_fk, has_sql]
        no_join_keys = all(not c for c in all_join_arguments)
        multiple_join_keys = sum(all_join_arguments) > 1

        if no_join_keys:
            raise ValueError(
                f"No join arguments found in join {definition['name']}, please pass sql_on or foreign_key"
            )

        if multiple_join_keys:
            raise ValueError(
                f"Multiple join arguments found in join {definition['name']}"
                ", please pass only one of: sql_on, foreign_key"
            )
        super().__init__(definition)

    def collect_errors(self):
        errors = []
        if self.foreign_key:
            for view_name in [self.from_, self.explore.from_]:
                try:
                    self.project.get_field(
                        self.foreign_key,
                        view_name=view_name,
                        explore_name=self.explore.name,
                        show_excluded=True,
                    )
                except Exception:
                    errors.append(
                        f"Could not find field {self.foreign_key} in join {self.name} "
                        f"referencing view {view_name} in explore {self.explore.name}"
                    )
            return errors

        fields_to_replace = self.fields_to_replace(self.sql_on)

        for field in fields_to_replace:
            _, join_name, column_name = Field.field_name_parts(field)
            view_name = self._resolve_view_name(join_name)
            try:
                view = self._get_view_internal(view_name)
            except Exception:
                err_msg = f"Could not find view {view_name} in join {self.name}"
                errors.append(err_msg)
                continue

            try:
                self.project.get_field(
                    column_name,
                    view_name=view.name,
                    explore_name=self.explore.name,
                    show_excluded=True,
                )
            except Exception:
                errors.append(
                    f"Could not find field {column_name} in join {self.name} "
                    f"referencing view {view_name} in explore {self.explore.name}"
                )
        return errors

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
        is_valid = self.foreign_key is not None
        return is_valid

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

    def required_joins(self):
        if not self.sql_on:
            return [self.explore.name, self.name]

        joins = []
        for field in self.fields_to_replace(self.sql_on):
            _, join_name, _ = Field.field_name_parts(field)
            joins.append(join_name)
        return list(set(joins))

    def to_dict(self):
        output = {**self._definition}
        return output

    def get_replaced_sql_on(self, sql: str, query_type: str):
        sql_on = deepcopy(sql)
        fields_to_replace = self.fields_to_replace(sql_on)

        for field in fields_to_replace:
            _, join_name, column_name = Field.field_name_parts(field)
            view_name = self._resolve_view_name(join_name)
            view = self._get_view_internal(view_name)

            if view is None:
                return

            table_name = view.name
            field_obj = self.project.get_field(
                column_name, view_name=table_name, explore_name=self.explore.name, show_excluded=True
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

    def _resolve_view_name(self, join_name: str):
        if join_name == self.explore.name:
            view_name = self.explore.from_
        else:
            join = self.explore.get_join(join_name)
            view_name = join.from_
        return view_name

    def _get_view_internal(self, view_name: str):
        if self.from_ is not None and view_name == self.from_:
            view = self.project.get_view(self.from_)
        elif view_name == self.explore.from_:
            view = self.project.get_view(self.explore.from_)
        else:
            view = self.project.get_view(view_name)
        return view

    def field_names(self):
        # This function is for the explore `fields` parameter, to resolve all the sets into field names
        if self.fields is None:
            return self.fields

        set_definition = {
            "name": "NA",
            "fields": self.fields,
            "view_name": self.from_,
            "explore_name": self.explore.name,
        }
        join_set = Set(set_definition, project=self.project, explore=self.explore)
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
            return [f for f in fields if f.id(view_only=True) in join_field_names]
        return fields
