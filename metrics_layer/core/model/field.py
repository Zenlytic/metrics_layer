import hashlib
import re
from copy import deepcopy

from .base import MetricsLayerBase, SQLReplacement
from .definitions import Definitions
from .filter import Filter
from .set import Set

SQL_KEYWORDS = {"order", "group", "by", "as", "from", "select", "on", "with"}


class Field(MetricsLayerBase, SQLReplacement):
    def __init__(self, definition: dict = {}, view=None) -> None:
        self.defaults = {"type": "string", "primary_key": "no", "datatype": "timestamp"}
        self.default_intervals = ["second", "minute", "hour", "day", "week", "month", "quarter", "year"]

        # Always lowercase names and make exception for the None case in the
        # event of this being used by a filter and not having a name.
        if definition["name"] is not None:
            definition["name"] = definition["name"].lower()

        if "primary_key" in definition and isinstance(definition["primary_key"], bool):
            definition["primary_key"] = "yes" if definition["primary_key"] else "no"

        if "hidden" in definition and isinstance(definition["hidden"], bool):
            definition["hidden"] = "yes" if definition["hidden"] else "no"

        if (
            "sql" not in definition
            and definition.get("field_type") == "measure"
            and definition.get("type") == "count"
        ):
            definition["primary_key_count"] = True

        self.view = view
        self.validate(definition)
        super().__init__(definition)

    def __hash__(self) -> int:
        result = hashlib.md5(self.id(view_only=True).encode("utf-8"))
        return int(result.hexdigest(), base=16)

    def __eq__(self, other):
        if isinstance(other, str):
            return False
        return self.id(view_only=True) == other.id(view_only=True)

    def id(self, view_only=False, capitalize_alias=False):
        alias = self.alias()
        if capitalize_alias:
            alias = alias.upper()
        if self.view.explore and not view_only:
            return f"{self.view.explore.name}.{self.view.name}.{alias}"
        return f"{self.view.name}.{alias}"

    @property
    def sql(self):
        definition = deepcopy(self._definition)
        if "sql" not in definition and "case" in definition:
            definition["sql"] = self._translate_looker_case_to_sql(definition["case"])

        if (
            "sql" not in definition
            and definition.get("field_type") == "measure"
            and definition.get("type") == "count"
        ):
            if self.view.primary_key:
                definition["sql"] = self.view.primary_key.sql
            else:
                definition["sql"] = "*"

        if "sql" in definition and "filters" in definition:
            if definition["sql"] == "*":
                raise ValueError(
                    "To apply filters to a count measure you must have the primary_key specified "
                    "for the view. You can do this by adding the tag 'primary_key: yes' to the "
                    "necessary dimension"
                )
            definition["sql"] = Filter.translate_looker_filters_to_sql(
                definition["sql"], definition["filters"]
            )

        if "sql" in definition and definition.get("type") == "tier":
            definition["sql"] = self._translate_looker_tier_to_sql(definition["sql"], definition["tiers"])

        if "sql" in definition:
            definition["sql"] = self._clean_sql_for_case(definition["sql"])
        return definition.get("sql")

    @property
    def label(self):
        if "label" in self._definition:
            label = self._definition["label"]
            if self.type == "time" and self.dimension_group:
                return f"{label} {self.dimension_group.replace('_', ' ').title()}"
            elif self.type == "duration" and self.dimension_group:
                return f"{self.dimension_group.replace('_', ' ').title()} {label}"
            return label
        return self.alias().replace("_", " ").title()

    @property
    def datatype(self):
        if "datatype" in self._definition:
            return self._definition["datatype"]
        elif self._definition["field_type"] == "dimension_group":
            return self.defaults["datatype"]
        return

    @property
    def drill_fields(self):
        drill_fields = self._definition.get("drill_fields")
        if drill_fields:
            set_definition = {"name": "drill_fields", "fields": drill_fields, "view_name": self.view.name}
            return Set(set_definition, project=self.view.project).field_names()
        return drill_fields

    def alias(self, with_view: bool = False):
        if self.field_type == "dimension_group":
            if self.type == "time":
                alias = f"{self.name}_{self.dimension_group}"
            elif self.type == "duration":
                alias = f"{self.dimension_group}_{self.name}"
        else:
            alias = self.name
        if with_view:
            return f"{self.view.name}_{alias}"
        return alias

    def sql_query(self, query_type: str = None, functional_pk: str = None, alias_only: bool = False):
        if not query_type:
            query_type = self._derive_query_type()
        if self.field_type == "measure":
            return self.aggregate_sql_query(query_type, functional_pk, alias_only=alias_only)
        return self.raw_sql_query(query_type, alias_only=alias_only)

    def raw_sql_query(self, query_type: str, alias_only: bool = False):
        if self.field_type == "measure" and self.type == "number":
            return self.get_referenced_sql_query()
        elif alias_only:
            return self.alias(with_view=True)
        return self.get_replaced_sql_query(query_type, alias_only=alias_only)

    def aggregate_sql_query(self, query_type: str, functional_pk: str, alias_only: bool = False):
        # TODO add median, median_distinct, percentile, max, min, percentile, percentile_distinct
        sql = self.raw_sql_query(query_type, alias_only=alias_only)
        type_lookup = {
            "sum": self._sum_aggregate_sql,
            "sum_distinct": self._sum_distinct_aggregate_sql,
            "count": self._count_aggregate_sql,
            "count_distinct": self._count_distinct_aggregate_sql,
            "average": self._average_aggregate_sql,
            "average_distinct": self._average_distinct_aggregate_sql,
            "number": self._number_aggregate_sql,
        }
        return type_lookup[self.type](sql, query_type, functional_pk, alias_only)

    def strict_replaced_query(self):
        clean_sql = deepcopy(self.sql)
        fields_to_replace = self.fields_to_replace(clean_sql)
        for to_replace in fields_to_replace:
            if to_replace == "TABLE":
                clean_sql = clean_sql.replace("${TABLE}.", "")
            else:
                field = self.get_field_with_view_info(to_replace, ignore_explore=True)
                if field:
                    sql_replace = field.alias(with_view=True)
                else:
                    sql_replace = to_replace

                clean_sql = clean_sql.replace("${" + to_replace + "}", sql_replace)
        return clean_sql.strip()

    def _needs_symmetric_aggregate(self, functional_pk: MetricsLayerBase):
        if functional_pk:
            if functional_pk == Definitions.does_not_exist:
                return True
            field_pk_id = self.view.primary_key.id(view_only=True)
            different_functional_pk = field_pk_id != functional_pk.id(view_only=True)
        else:
            different_functional_pk = False
        return different_functional_pk

    def _count_distinct_aggregate_sql(self, sql: str, query_type: str, functional_pk: str, alias_only: bool):
        return f"COUNT(DISTINCT({sql}))"

    def _sum_aggregate_sql(self, sql: str, query_type: str, functional_pk: str, alias_only: bool):
        if self._needs_symmetric_aggregate(functional_pk):
            return self._sum_symmetric_aggregate(sql, query_type, alias_only=alias_only)
        return f"SUM({sql})"

    def _sum_distinct_aggregate_sql(self, sql: str, query_type: str, functional_pk: str, alias_only: bool):
        sql_distinct_key = self._replace_sql_query(self.sql_distinct_key, query_type, alias_only=alias_only)
        return self._sum_symmetric_aggregate(
            sql, query_type, primary_key_sql=sql_distinct_key, alias_only=alias_only
        )

    def _sum_symmetric_aggregate(
        self,
        sql: str,
        query_type: str,
        primary_key_sql: str = None,
        alias_only: bool = False,
        factor: int = 1_000_000,
    ):
        if query_type in {Definitions.snowflake, Definitions.redshift}:
            return self._sum_symmetric_aggregate_snowflake(sql, primary_key_sql, alias_only, factor)
        elif query_type == Definitions.bigquery:
            return self._sum_symmetric_aggregate_bigquery(sql, primary_key_sql, alias_only, factor)

    def _sum_symmetric_aggregate_bigquery(
        self, sql: str, primary_key_sql: str, alias_only: bool, factor: int = 1_000_000
    ):
        if not primary_key_sql:
            primary_key_sql = self.view.primary_key.sql_query(Definitions.bigquery, alias_only=alias_only)

        adjusted_sum = f"(CAST(FLOOR(COALESCE({sql}, 0) * ({factor} * 1.0)) AS FLOAT64))"

        pk_sum = f"CAST(FARM_FINGERPRINT(CAST({primary_key_sql} AS STRING)) AS BIGNUMERIC)"

        sum_with_pk_backout = f"SUM(DISTINCT {adjusted_sum} + {pk_sum}) - SUM(DISTINCT {pk_sum})"

        backed_out_cast = f"COALESCE(CAST(({sum_with_pk_backout}) AS FLOAT64)"

        result = f"{backed_out_cast} / CAST(({factor}*1.0) AS FLOAT64), 0)"
        return result

    def _sum_symmetric_aggregate_snowflake(
        self, sql: str, primary_key_sql: str, alias_only: bool, factor: int = 1_000_000
    ):
        if not primary_key_sql:
            primary_key_sql = self.view.primary_key.sql_query(Definitions.snowflake, alias_only=alias_only)

        adjusted_sum = f"(CAST(FLOOR(COALESCE({sql}, 0) * ({factor} * 1.0)) AS DECIMAL(38,0)))"

        pk_sum = f"(TO_NUMBER(MD5({primary_key_sql}), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)"  # noqa

        sum_with_pk_backout = f"SUM(DISTINCT {adjusted_sum} + {pk_sum}) - SUM(DISTINCT {pk_sum})"

        backed_out_cast = f"COALESCE(CAST(({sum_with_pk_backout}) AS DOUBLE PRECISION)"

        result = f"{backed_out_cast} / CAST(({factor}*1.0) AS DOUBLE PRECISION), 0)"
        return result

    def _count_aggregate_sql(self, sql: str, query_type: str, functional_pk: str, alias_only: bool):
        if self._needs_symmetric_aggregate(functional_pk):
            if self.primary_key_count:
                return self._count_distinct_aggregate_sql(
                    sql, query_type, functional_pk, alias_only=alias_only
                )
            return self._count_symmetric_aggregate(sql, query_type, alias_only=alias_only)
        return f"COUNT({sql})"

    def _count_symmetric_aggregate(
        self,
        sql: str,
        query_type: str,
        primary_key_sql: str = None,
        alias_only: bool = False,
        factor: int = 1_000_000,
    ):
        # This works for both query types
        return self._count_symmetric_aggregate_snowflake(
            sql, query_type, primary_key_sql=primary_key_sql, alias_only=alias_only
        )

    def _count_symmetric_aggregate_snowflake(
        self, sql: str, query_type: str, primary_key_sql: str, alias_only: bool
    ):
        if not primary_key_sql:
            primary_key_sql = self.view.primary_key.sql_query(query_type, alias_only=alias_only)
        pk_if_not_null = f"CASE WHEN  ({sql})  IS NOT NULL THEN  {primary_key_sql}  ELSE NULL END"
        result = f"NULLIF(COUNT(DISTINCT {pk_if_not_null}), 0)"
        return result

    def _average_aggregate_sql(self, sql: str, query_type: str, functional_pk: str, alias_only: bool):
        if self._needs_symmetric_aggregate(functional_pk):
            return self._average_symmetric_aggregate(sql, query_type, alias_only=alias_only)
        return f"AVG({sql})"

    def _average_distinct_aggregate_sql(
        self, sql: str, query_type: str, functional_pk: str, alias_only: bool
    ):
        sql_distinct_key = self._replace_sql_query(self.sql_distinct_key, query_type, alias_only=alias_only)
        return self._average_symmetric_aggregate(
            sql, query_type, primary_key_sql=sql_distinct_key, alias_only=alias_only
        )

    def _average_symmetric_aggregate(
        self, sql: str, query_type, primary_key_sql: str = None, alias_only: bool = False
    ):
        sum_symmetric = self._sum_symmetric_aggregate(
            sql, query_type, primary_key_sql=primary_key_sql, alias_only=alias_only
        )
        count_symmetric = self._count_symmetric_aggregate(
            sql, query_type, primary_key_sql=primary_key_sql, alias_only=alias_only
        )
        result = f"({sum_symmetric} / {count_symmetric})"
        return result

    def _number_aggregate_sql(self, sql: str, query_type: str, functional_pk: str, alias_only: bool):
        if isinstance(sql, list):
            replaced = deepcopy(self.sql)
            for field_name in self.fields_to_replace(self.sql):
                proper_to_replace = "${" + field_name + "}"
                if field_name == "TABLE":
                    if alias_only:
                        proper_to_replace += "."
                        to_replace = ""
                    else:
                        to_replace = self.view.name
                else:
                    field = self.get_field_with_view_info(field_name)
                    to_replace = field.sql_query(query_type, functional_pk, alias_only=alias_only)
                replaced = replaced.replace(proper_to_replace, to_replace)
        else:
            raise ValueError(f"handle case for sql: {sql}")
        return replaced

    def required_views(self):
        views = []
        if self.sql:
            views.extend(self._get_required_views_from_sql(self.sql))
        elif self.sql_start and self.sql_end:
            views.extend(self._get_required_views_from_sql(self.sql_start))
            views.extend(self._get_required_views_from_sql(self.sql_end))
        else:
            # There is not sql or sql_start or sql_end, it must be a
            # default count measure which references only the field's base view
            pass

        return list(set([self.view.name] + views))

    def _get_required_views_from_sql(self, sql: str):
        views = []
        for field_name in self.fields_to_replace(sql):
            if field_name != "TABLE":
                field = self.get_field_with_view_info(field_name)
                views.extend(field.required_views())
        return views

    def validate(self, definition: dict):
        required_keys = ["name", "field_type"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Field missing required key '{k}' The field passed was {definition}")

    def to_dict(self, query_type: str = None):
        output = {**self._definition}
        output["sql_raw"] = deepcopy(self.sql)
        if output["field_type"] == "measure" and output["type"] == "number":
            output["sql"] = self.get_referenced_sql_query()
        elif output["field_type"] == "dimension_group" and self.dimension_group is None:
            output["sql"] = deepcopy(self.sql)
        elif query_type:
            output["sql"] = self.sql_query(query_type)
        return output

    def printable_attributes(self):
        to_print = [
            "name",
            "field_type",
            "type",
            "label",
            "group_label",
            "hidden",
            "primary_key",
            "timeframes",
            "datatype",
            "sql",
        ]
        attributes = self.to_dict()
        return {key: attributes.get(key) for key in to_print if attributes.get(key) is not None}

    def equal(self, field_name: str):
        # Determine if the field name passed in references this field
        _, view_name, field_name_only = self.field_name_parts(field_name)
        if view_name and view_name != self.view.name:
            return False

        if self.field_type == "dimension_group" and self.dimension_group is None:
            if field_name_only in self.dimension_group_names():
                self.dimension_group = self.get_dimension_group_name(field_name_only)
                return True
            return False
        elif self.field_type == "dimension_group":
            return self.alias() == field_name_only
        return self.name == field_name_only

    def dimension_group_names(self):
        if self.field_type == "dimension_group" and self.type == "time":
            return [f"{self.name}_{t}" for t in self._definition.get("timeframes", [])]
        if self.field_type == "dimension_group" and self.type == "duration":
            return [f"{t}s_{self.name}" for t in self._definition.get("intervals", self.default_intervals)]
        return []

    def get_dimension_group_name(self, field_name: str):
        if self.type == "duration" and f"_{self.name}" in field_name:
            return field_name.replace(f"_{self.name}", "")
        if self.type == "time":
            return field_name.replace(f"{self.name}_", "")
        return None

    def apply_dimension_group_duration_sql(self, sql_start: str, sql_end: str, query_type: str):
        meta_lookup = {
            Definitions.snowflake: {
                "seconds": lambda start, end: f"DATEDIFF('SECOND', {start}, {end})",
                "minutes": lambda start, end: f"DATEDIFF('MINUTE', {start}, {end})",
                "hours": lambda start, end: f"DATEDIFF('HOUR', {start}, {end})",
                "days": lambda start, end: f"DATEDIFF('DAY', {start}, {end})",
                "weeks": lambda start, end: f"DATEDIFF('WEEK', {start}, {end})",
                "months": lambda start, end: f"DATEDIFF('MONTH', {start}, {end})",
                "quarters": lambda start, end: f"DATEDIFF('QUARTER', {start}, {end})",
                "years": lambda start, end: f"DATEDIFF('YEAR', {start}, {end})",
            },
            Definitions.bigquery: {
                "days": lambda start, end: f"DATE_DIFF(CAST({end} as DATE), CAST({start} as DATE), DAY)",
                "weeks": lambda start, end: f"DATE_DIFF(CAST({end} as DATE), CAST({start} as DATE), ISOWEEK)",
                "months": lambda start, end: f"DATE_DIFF(CAST({end} as DATE), CAST({start} as DATE), MONTH)",
                "quarters": lambda start, end: f"DATE_DIFF(CAST({end} as DATE), CAST({start} as DATE), QUARTER)",  # noqa
                "years": lambda start, end: f"DATE_DIFF(CAST({end} as DATE), CAST({start} as DATE), ISOYEAR)",
            },
        }
        # Snowflake and redshift have identical syntax in this case
        meta_lookup[Definitions.redshift] = meta_lookup[Definitions.snowflake]
        try:
            return meta_lookup[query_type][self.dimension_group](sql_start, sql_end)
        except KeyError:
            raise KeyError(
                f"Unable to find a valid method for running "
                f"{self.dimension_group} with query type {query_type}"
            )

    def apply_dimension_group_time_sql(self, sql: str, query_type: str):
        # TODO add day_of_week, day_of_week_index, month_name, month_num
        # more types here https://docs.looker.com/reference/field-params/dimension_group
        meta_lookup = {
            Definitions.snowflake: {
                "raw": lambda s, qt: s,
                "time": lambda s, qt: f"CAST({s} as TIMESTAMP)",
                "date": lambda s, qt: f"DATE_TRUNC('DAY', {s})",
                "week": self._week_dimension_group_time_sql,
                "month": lambda s, qt: f"DATE_TRUNC('MONTH', {s})",
                "quarter": lambda s, qt: f"DATE_TRUNC('QUARTER', {s})",
                "year": lambda s, qt: f"DATE_TRUNC('YEAR', {s})",
                "hour_of_day": lambda s, qt: f"HOUR({s})",
                "day_of_week": lambda s, qt: f"DAYOFWEEK({s})",
            },
            Definitions.bigquery: {
                "raw": lambda s, qt: s,
                "time": lambda s, qt: f"CAST({s} as TIMESTAMP)",
                "date": lambda s, qt: f"CAST(DATE_TRUNC(CAST({s} as DATE), DAY) AS {self.datatype.upper()})",
                "week": self._week_dimension_group_time_sql,
                "month": lambda s, qt: f"CAST(DATE_TRUNC(CAST({s} as DATE), MONTH) AS {self.datatype.upper()})",  # noqa
                "quarter": lambda s, qt: f"CAST(DATE_TRUNC(CAST({s} as DATE), QUARTER) AS {self.datatype.upper()})",  # noqa
                "year": lambda s, qt: f"CAST(DATE_TRUNC(CAST({s} as DATE), YEAR) AS {self.datatype.upper()})",
                "hour_of_day": lambda s, qt: f"CAST({s} AS STRING FORMAT 'HH24')",
                "day_of_week": lambda s, qt: f"CAST({s} AS STRING FORMAT 'DAY')",
            },
        }
        # Snowflake and redshift have identical syntax in this case
        meta_lookup[Definitions.redshift] = meta_lookup[Definitions.snowflake]

        return meta_lookup[query_type][self.dimension_group](sql, query_type)

    def _week_dimension_group_time_sql(self, sql: str, query_type: str):
        # Monday is the default date for warehouses
        week_start_day = self.view.explore.week_start_day
        if week_start_day == "monday":
            return self._week_sql_date_trunc(sql, None, query_type)
        offset_lookup = {
            "sunday": 1,
            "saturday": 2,
            "friday": 3,
            "thursday": 4,
            "wednesday": 5,
            "tuesday": 6,
        }
        offset = offset_lookup[week_start_day]
        positioned_sql = f"{self._week_sql_date_trunc(sql, offset, query_type)} - {offset}"
        if query_type == Definitions.bigquery:
            positioned_sql = f"CAST({positioned_sql} AS {self.datatype.upper()})"
        return positioned_sql

    @staticmethod
    def _week_sql_date_trunc(sql, offset, query_type):
        if query_type in {Definitions.snowflake, Definitions.redshift}:
            if offset is None:
                offset_sql = sql
            else:
                offset_sql = f"{sql} + {offset}"
            return f"DATE_TRUNC('WEEK', {offset_sql})"
        elif Definitions.bigquery == query_type:
            if offset is None:
                return f"DATE_TRUNC(CAST({sql} as DATE), WEEK)"
            return f"DATE_TRUNC(CAST({sql} as DATE) + {offset}, WEEK)"

    def collect_errors(self):
        if not self.valid_name(self.name):
            return [self.name_error("field", self.name)]
        return []

    def get_referenced_sql_query(self, strings_only=True):
        if self.sql and ("{%" in self.sql or self.sql == ""):
            return None

        if self.sql_start and self.sql_end and self.type == "duration":
            start_fields = self.referenced_fields(self.sql_start)
            end_fields = self.referenced_fields(self.sql_end)
            referenced_fields = start_fields + end_fields
        else:
            referenced_fields = self.referenced_fields(self.sql)

        if strings_only:
            valid_references = [f for f in referenced_fields if not isinstance(f, str)]
            return list(set(f"{f.view.name}.{f.name}" for f in valid_references))
        return referenced_fields

    def referenced_fields(self, sql, ignore_explore: bool = False):
        reference_fields = []
        for to_replace in self.fields_to_replace(sql):
            if to_replace != "TABLE":
                try:
                    field = self.get_field_with_view_info(to_replace, ignore_explore=ignore_explore)
                except Exception:
                    field = None
                to_replace_type = None if field is None else field.type

                if to_replace_type == "number":
                    reference_fields.extend(field.get_referenced_sql_query(strings_only=False))
                elif to_replace_type is None and field is None:
                    reference_fields.append(to_replace)
                else:
                    reference_fields.append(field)

        return reference_fields

    def get_replaced_sql_query(self, query_type: str, alias_only: bool = False):
        if self.sql:
            clean_sql = self._replace_sql_query(self.sql, query_type, alias_only=alias_only)
            if self.field_type == "dimension_group" and self.type == "time":
                clean_sql = self.apply_dimension_group_time_sql(clean_sql, query_type)
            return clean_sql

        if self.sql_start and self.sql_end and self.type == "duration":
            clean_sql_start = self._replace_sql_query(self.sql_start, query_type, alias_only=alias_only)
            clean_sql_end = self._replace_sql_query(self.sql_end, query_type, alias_only=alias_only)
            return self.apply_dimension_group_duration_sql(clean_sql_start, clean_sql_end, query_type)

        raise ValueError(f"Unknown type of SQL query for field {self.name}")

    def _replace_sql_query(self, sql_query: str, query_type: str, alias_only: bool = False):
        if sql_query is None or "{%" in sql_query or sql_query == "":
            return None
        clean_sql = self.replace_fields(sql_query, query_type, alias_only=alias_only)
        clean_sql = re.sub(r"[ ]{2,}", " ", clean_sql)
        clean_sql = clean_sql.replace("'", "'")
        return clean_sql

    def replace_fields(self, sql, query_type, view_name=None, alias_only=False):
        clean_sql = deepcopy(sql)
        view_name = self.view.name if not view_name else view_name
        fields_to_replace = self.fields_to_replace(sql)
        for to_replace in fields_to_replace:
            if to_replace == "TABLE":
                if alias_only:
                    clean_sql = clean_sql.replace("${TABLE}.", "")
                else:
                    clean_sql = clean_sql.replace("${TABLE}", view_name)
            else:
                field = self.get_field_with_view_info(to_replace, specified_view=view_name)
                if field:
                    sql_replace = field.raw_sql_query(query_type, alias_only=alias_only)
                else:
                    sql_replace = to_replace

                clean_sql = clean_sql.replace("${" + to_replace + "}", sql_replace)
        return clean_sql.strip()

    def get_field_with_view_info(self, field: str, specified_view: str = None, ignore_explore: bool = False):
        specified_explore, view_name, field_name = self.field_name_parts(field)
        if view_name is None and specified_view is None:
            view_name = self.view.name
        elif view_name is None and specified_view:
            view_name = specified_view

        if self.view is None:
            raise AttributeError(f"You must specify which view this field is in '{self.name}'")
        if self.view.explore and not ignore_explore:
            explore_name = self.view.explore.name
        else:
            explore_name = specified_explore
        return self.view.project.get_field(field_name, view_name=view_name, explore_name=explore_name)

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

    def _derive_query_type(self):
        explore = self.view.explore
        if explore is None:
            raise ValueError(
                f"Could not find a explore in field {self.alias()} to use to detect the query type, "
                "please pass the query type explicitly using the query_type argument"
                "or pass an explore name using the explore_name argument"
            )
        connection_type = self.view.project.connection_lookup.get(explore.model.connection)
        if connection_type is None:
            raise ValueError(
                f"Could not find the connection named {explore.model.connection} "
                f"in explore {explore.name} to use in detecting the query type, "
                "please pass the query type explicitly using the query_type argument"
            )
        return connection_type

    @staticmethod
    def _name_is_not_valid_sql(name: str):
        name_is_keyword = name is not None and name.lower() in SQL_KEYWORDS
        digit_first_char = name[0] in {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
        return name_is_keyword or digit_first_char
