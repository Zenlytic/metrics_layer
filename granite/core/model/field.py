import re
from copy import deepcopy

from .base import GraniteBase, SQLReplacement


class Field(GraniteBase, SQLReplacement):
    def __init__(self, definition: dict = {}, view=None) -> None:
        self.defaults = {"type": "string", "primary_key": "no"}

        # definition["name"] = definition["name"].lower()
        # if definition["name"][0] in {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}:
        #     definition["name"] = "_" + definition["name"]
        # if "sql" in definition:
        #     # TODO clean up - this is pretty hacky
        #     definition["sql"] = definition["sql"].lower().replace("${table}", "${TABLE}")

        # if "value_format" in definition and "value_format_name" not in definition:
        #     new_value_format_name = self._derived_value_format_name(definition["value_format"])
        #     if new_value_format_name:
        #         definition["value_format_name"] = new_value_format_name
        self.validate(definition)
        self.view = view
        super().__init__(definition)

    def alias(self):
        return self.name

    def sql_query(self):
        if self.field_type == "measure":
            return self.aggregate_sql_query()
        return self.raw_sql_query()

    def raw_sql_query(self):
        if self.field_type == "measure" and self.type == "number":
            return self.get_referenced_sql_query()
        return self.get_replaced_sql_query()

    def aggregate_sql_query(self):
        sql = self.raw_sql_query()
        type_lookup = {
            "sum": lambda s: f"SUM({s})",
            "count_distinct": lambda s: f"COUNT(DISTINCT({s}))",
            "count": lambda s: f"COUNT({s})",
            "average": lambda s: f"AVG({s})",
            "number": self._number_aggregate_sql,
        }
        return type_lookup[self.type](sql)

    def _number_aggregate_sql(self, sql: str):
        if isinstance(sql, list):
            replaced = deepcopy(self.sql)
            for field_name in self.fields_to_replace(self.sql):
                field = self.get_field_with_view_info(field_name)
                replaced = replaced.replace("${" + field.name + "}", field.aggregate_sql_query())
        else:
            raise ValueError(f"handle case for sql: {sql}")
        return replaced

    def validate(self, definition: dict):
        required_keys = ["name", "field_type", "sql"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Field missing required key {k}")

    def to_dict(self):
        output = {**self._definition}
        output["sql_raw"] = deepcopy(self.sql)
        if output["field_type"] == "measure" and output["type"] == "number":
            output["sql"] = self.get_referenced_sql_query()
        else:
            output["sql"] = self.get_replaced_sql_query()
        return output

    def equal(self, field_name: str):
        # Determine if the field name passed in references this field
        if self.field_type == "dimension_group":
            if field_name in self.dimension_group_names():
                self.dimension_group = self.get_dimension_group_name(field_name)
                return True
            return False
        return self.name == field_name

    # def parse_function_definition(self):
    #     if self.field_type == "measure":
    #         return FunctionDefinitionParser(self, self.project).parse()
    #     return [], None

    def dimension_group_names(self):
        if self.field_type == "dimension_group":
            return [f"{self.name}_{t}" for t in self._definition.get("timeframes", [])] + [self.name]
        return []

    def get_dimension_group_name(self, field_name: str):
        return field_name.replace(f"{self.name}_", "")

    def apply_dimension_group_sql(self, sql: str):
        dimension_group_sql_lookup = {
            "raw": lambda s: s,
            "date": lambda s: f"DATE_TRUNC('DAY', {s})",
            "week": lambda s: f"DATE_TRUNC('WEEK', {s})",
            "month": lambda s: f"DATE_TRUNC('MONTH', {s})",
            "quarter": lambda s: f"DATE_TRUNC('QUARTER', {s})",
            "year": lambda s: f"DATE_TRUNC('YEAR', {s})",
        }
        if self.dimension_group:
            return dimension_group_sql_lookup[self.dimension_group](sql)
        return sql

    def get_referenced_sql_query(self):
        if "{%" in self.sql or self.sql == "":
            return None
        return self.reference_fields(self.sql)

    def reference_fields(self, sql):
        reference_fields = []
        for to_replace in self.fields_to_replace(sql):
            if to_replace != "TABLE":
                field = self.get_field_with_view_info(to_replace)
                to_replace_type = None if field is None else field.type

                if to_replace_type == "number":
                    sql_replace = deepcopy(field.sql)
                    sql_replace = to_replace if sql_replace is None else sql_replace
                    reference_fields.extend(self.reference_fields(sql_replace))
                else:
                    reference_fields.append(to_replace)

        return list(set(reference_fields))

    def get_replaced_sql_query(self):
        if self.sql is None or "{%" in self.sql or self.sql == "":
            return None
        clean_sql = self.replace_fields(self.sql)
        clean_sql = re.sub(r"[ ]{2,}", " ", clean_sql)
        clean_sql = clean_sql.replace("\n", "").replace("'", "'")
        # We must add the DATE() or DATE_TRUNC('month') part to the dimension
        if self.field_type == "dimension_group":
            clean_sql = self.apply_dimension_group_sql(clean_sql)
        return clean_sql

    def replace_fields(self, sql, view_name=None):
        clean_sql = deepcopy(sql)
        view_name = self.view.name if not view_name else view_name
        fields_to_replace = self.fields_to_replace(sql)
        for to_replace in fields_to_replace:
            if to_replace == "TABLE":
                clean_sql = clean_sql.replace("${TABLE}", view_name)
            else:
                field = self.get_field_with_view_info(to_replace)
                sql_replace = deepcopy(field.sql) if field and field.sql else to_replace
                clean_sql = clean_sql.replace(
                    "${" + to_replace + "}", self.replace_fields(sql_replace, view_name=field.view_name)
                )
        return clean_sql.strip()

    def get_field_with_view_info(self, field: str):
        if "." in field:
            view_name, field_name = field.split(".")
        else:
            view_name, field_name = self.view.name, field
        if self.view is None:
            raise AttributeError(f"You must specify which view this field is in '{self.name}'")
        return self.view.project.get_field(field_name, view_name=view_name)

    @staticmethod
    def _derived_value_format_name(value_format: str):
        value_format_lookup = {
            "0": "decimal_0",
            "#,##0": "decimal_0",
            "0.#": "decimal_1",
            "0.0": "decimal_1",
            "#,##0.0": "decimal_1",
            "0.##": "decimal_2",
            "0.00": "decimal_2",
            "#,##0.00": "decimal_2",
            "$0": "usd_0",
            "$0.00": "usd_2",
            "$#,##0": "usd_0",
            "$#,##0.0": "usd_1",
            "$#,##0.00": "usd_2",
            "0\%": "percent_0",  # noqa
            "0.0\%": "percent_1",  # noqa
            "0.00\%": "percent_2",  # noqa
        }
        return value_format_lookup.get(value_format)
