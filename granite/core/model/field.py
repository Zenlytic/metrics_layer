import re
from copy import deepcopy

from .base import GraniteBase, SQLReplacement


class Field(GraniteBase, SQLReplacement):
    def __init__(self, definition: dict = {}, view=None) -> None:
        self.defaults = {"type": "string", "primary_key": "no"}
        self.default_intervals = ["second", "minute", "hour", "day", "week", "month", "quarter", "year"]

        # Always lowercase names and make exception for the None case in the
        # event of this being used by a filter and not having a name.
        if definition["name"] is not None:
            definition["name"] = definition["name"].lower()

        if "primary_key" in definition and isinstance(definition["primary_key"], bool):
            definition["primary_key"] = "yes" if definition["primary_key"] else "no"

        if "hidden" in definition and isinstance(definition["hidden"], bool):
            definition["hidden"] = "yes" if definition["hidden"] else "no"

        # TODO figure out how to handle this weird case
        # if definition["name"][0] in {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}:
        #     definition["name"] = "_" + definition["name"]

        self.view = view
        self.validate(definition)
        super().__init__(definition)

    @property
    def sql(self):
        definition = deepcopy(self._definition)
        if "sql" not in definition and "case" in definition:
            definition["sql"] = self._translate_looker_case_to_sql(definition["case"])

        if "sql" in definition and "filters" in definition:
            definition["sql"] = self._translate_looker_filter_to_sql(definition["sql"], definition["filters"])

        if "sql" in definition and definition.get("type") == "tier":
            definition["sql"] = self._translate_looker_tier_to_sql(definition["sql"], definition["tiers"])

        if "sql" in definition:
            definition["sql"] = self._clean_sql_for_case(definition["sql"])
        return definition.get("sql")

    def alias(self):
        if self.field_type == "dimension_group":
            if self.type == "time":
                return f"{self.name}_{self.dimension_group}"
            elif self.type == "duration":
                return f"{self.dimension_group}_{self.name}"
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
                if field_name == "TABLE":
                    to_replace = self.view.name
                else:
                    field = self.get_field_with_view_info(field_name)
                    to_replace = field.aggregate_sql_query()
                replaced = replaced.replace("${" + field_name + "}", to_replace)
        else:
            raise ValueError(f"handle case for sql: {sql}")
        return replaced

    def validate(self, definition: dict):
        required_keys = ["name", "field_type"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Field missing required key {k}")
        if all(key not in definition for key in ["sql", "sql_end", "sql_start", "case"]):
            raise ValueError(f"Field missing one of required keys sql, sql_start, sql_end, case")

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

    def dimension_group_names(self):
        if self.field_type == "dimension_group" and self.type == "time":
            return [f"{self.name}_{t}" for t in self._definition.get("timeframes", [])]
        if self.field_type == "dimension_group" and self.type == "duration":
            return [f"{t}s_{self.name}" for t in self._definition.get("intervals", self.default_intervals)]
        return []

    def get_dimension_group_name(self, field_name: str):
        if self.type == "duration":
            return field_name.replace(f"_{self.name}", "")
        if self.type == "time":
            return field_name.replace(f"{self.name}_", "")
        return self.name

    def apply_dimension_group_duration_sql(self, sql_start: str, sql_end: str):
        dimension_group_sql_lookup = {
            "seconds": lambda start, end: f"DATEDIFF('SECOND', {start}, {end})",
            "minutes": lambda start, end: f"DATEDIFF('MINUTE', {start}, {end})",
            "hours": lambda start, end: f"DATEDIFF('HOUR', {start}, {end})",
            "days": lambda start, end: f"DATEDIFF('DAY', {start}, {end})",
            "weeks": lambda start, end: f"DATEDIFF('WEEK', {start}, {end})",
            "months": lambda start, end: f"DATEDIFF('MONTH', {start}, {end})",
            "quarters": lambda start, end: f"DATEDIFF('QUARTER', {start}, {end})",
            "years": lambda start, end: f"DATEDIFF('YEAR', {start}, {end})",
        }
        return dimension_group_sql_lookup[self.dimension_group](sql_start, sql_end)

    def apply_dimension_group_time_sql(self, sql: str):
        # TODO add day_of_week, day_of_week_index, month_name, month_num
        # more types here https://docs.looker.com/reference/field-params/dimension_group
        dimension_group_sql_lookup = {
            "raw": lambda s: s,
            "time": lambda s: f"CAST({s} as TIMESTAMP)",
            "date": lambda s: f"DATE_TRUNC('DAY', {s})",
            "week": self._week_dimension_group_time_sql,
            "month": lambda s: f"DATE_TRUNC('MONTH', {s})",
            "quarter": lambda s: f"DATE_TRUNC('QUARTER', {s})",
            "year": lambda s: f"DATE_TRUNC('YEAR', {s})",
        }
        return dimension_group_sql_lookup[self.dimension_group](sql)

    def _week_dimension_group_time_sql(self, sql: str):
        # Monday is the default date for warehouses
        week_start_day = self.view.explore.week_start_day
        if week_start_day == "monday":
            return f"DATE_TRUNC('WEEK', {sql})"
        offset_lookup = {
            "sunday": 1,
            "saturday": 2,
            "friday": 3,
            "thursday": 4,
            "wednesday": 5,
            "tuesday": 6,
        }
        offset = offset_lookup[week_start_day]
        return f"DATE_TRUNC('WEEK', {sql} + {offset}) - {offset}"

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
        if self.sql:
            clean_sql = self._replace_sql_query(self.sql)
            if self.field_type == "dimension_group" and self.type == "time":
                clean_sql = self.apply_dimension_group_time_sql(clean_sql)
            return clean_sql

        if self.sql_start and self.sql_end and self.type == "duration":
            clean_sql_start = self._replace_sql_query(self.sql_start)
            clean_sql_end = self._replace_sql_query(self.sql_end)
            return self.apply_dimension_group_duration_sql(clean_sql_start, clean_sql_end)

        raise ValueError(f"Unknown type of SQL query for field {self.name}")

    def _replace_sql_query(self, sql_query: str):
        if sql_query is None or "{%" in sql_query or sql_query == "":
            return None
        clean_sql = self.replace_fields(sql_query)
        clean_sql = re.sub(r"[ ]{2,}", " ", clean_sql)
        clean_sql = clean_sql.replace("\n", "").replace("'", "'")
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
                    "${" + to_replace + "}", self.replace_fields(sql_replace, view_name=field.view.name)
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

    def _translate_looker_tier_to_sql(self, sql: str, tiers: list):
        case_sql = "case "
        when_sql = f"when {sql} < {tiers[0]} then 'Below {tiers[0]}' "
        case_sql += when_sql

        # Handle all bucketed conditions
        for i, tier in enumerate(tiers[:-1]):
            start, end = tier, tiers[i + 1]
            when_sql = f"when {sql} >= {start} and {sql} < {end} then '[{start},{end})' "
            case_sql += when_sql

        # Handle last condition greater than of equal to the last bucket cutoff
        when_sql = f"when {sql} >= {tiers[-1]} then '[{tiers[-1]},inf)' "
        case_sql += when_sql
        return case_sql + "else 'Unknown' end"

    @staticmethod
    def _translate_looker_filter_to_sql(sql: str, filters: list):
        case_sql = "case when "
        conditions = []
        for f in filters:
            # TODO add the advanced filter parsing here
            field_reference = "${" + f["field"] + "}"
            condition = f"{field_reference} = '{f['value']}'"
            conditions.append(condition)

        # Add the filter conditions AND'd together
        case_sql += " and ".join(conditions)
        # Add the result from the sql arg + imply NULL for anything not hitting the filter condition
        case_sql += f" then {sql} end"

        return case_sql

    @staticmethod
    def _translate_looker_case_to_sql(case: dict):
        case_sql = "case "
        for when in case["whens"]:
            # Do this so the warehouse doesn't think it's an identifier
            when_condition_sql = when["sql"].replace('"', "'")
            when_sql = f"when {when_condition_sql} then '{when['label']}' "
            case_sql += when_sql

        if case.get("else"):
            case_sql += f"else '{case['else']}' "

        return case_sql + "end"

    def _clean_sql_for_case(self, sql: str):
        clean_sql = deepcopy(sql)
        for to_replace in self.fields_to_replace(sql):
            if to_replace != "TABLE":
                clean_sql = clean_sql.replace("${" + to_replace + "}", "${" + to_replace.lower() + "}")
        return clean_sql
