import functools
import hashlib
import re
from copy import deepcopy
from pypika.terms import LiteralValue

from metrics_layer.core.exceptions import AccessDeniedOrDoesNotExistException, QueryError
from .base import MetricsLayerBase, SQLReplacement
from .definitions import Definitions
from .filter import Filter
from .set import Set

SQL_KEYWORDS = {"order", "group", "by", "as", "from", "select", "on", "with"}
VALID_TIMEFRAMES = [
    "raw",
    "time",
    "second",
    "minute",
    "hour",
    "date",
    "week",
    "month",
    "quarter",
    "year",
    "week_index",
    "week_of_year",
    "week_of_month",
    "month_of_year",
    "month_of_year_index",
    "month_name",
    "month_index",
    "quarter_of_year",
    "hour_of_day",
    "day_of_week",
    "day_of_month",
    "day_of_year",
]
VALID_INTERVALS = [
    "second",
    "minute",
    "hour",
    "day",
    "week",
    "month",
    "quarter",
    "year",
]
VALID_VALUE_FORMAT_NAMES = [
    "decimal_0",
    "decimal_1",
    "decimal_2",
    "decimal_pct_0",
    "decimal_pct_1",
    "decimal_pct_2",
    "percent_0",
    "percent_1",
    "percent_2",
    "eur",
    "eur_0",
    "eur_1",
    "eur_2",
    "usd",
    "usd_0",
    "usd_1",
    "usd_2",
    "string",
    "date",
    "week",
    "month",
    "quarter",
    "year",
]


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
        result = hashlib.md5(self.id().encode("utf-8"))
        id_int = int(result.hexdigest(), base=16)
        return hash(self.view.project) + id_int

    def __eq__(self, other):
        if isinstance(other, str):
            return False
        return self.id() == other.id()

    def id(self, capitalize_alias=False):
        alias = self.alias()
        if capitalize_alias:
            alias = alias.upper()
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

        if "sql" in definition and (self.filters or self.non_additive_dimension):
            if definition["sql"] == "*":
                raise QueryError(
                    "To apply filters to a count measure you must have the primary_key specified "
                    "for the view. You can do this by adding the tag 'primary_key: yes' to the "
                    "necessary dimension"
                )
            filters_to_apply = definition.get("filters", [])

            if non_additive_dimension := self.non_additive_dimension:
                filters_to_apply += [
                    {
                        "field": non_additive_dimension["name"],
                        "value": LiteralValue(f"{self.non_additive_cte_alias()}.{self.non_additive_alias()}"),
                    }
                ]
                for window_grouping in non_additive_dimension.get("window_groupings", []):
                    window_alias = window_grouping.replace(".", "_")
                    filters_to_apply += [
                        {
                            "field": window_grouping,
                            "value": LiteralValue(f"{self.non_additive_cte_alias()}.{window_alias}"),
                        }
                    ]
            definition["sql"] = Filter.translate_looker_filters_to_sql(definition["sql"], filters_to_apply)

        if "sql" in definition and definition.get("type") == "tier":
            definition["sql"] = self._translate_looker_tier_to_sql(definition["sql"], definition["tiers"])

        # We need to put parenthesis around yesno types
        if "sql" in definition and definition.get("type") == "yesno":
            definition["sql"] = f'({definition["sql"]})'

        if "sql" in definition:
            definition["sql"] = self._clean_sql_for_case(definition["sql"])
        return definition.get("sql")

    @property
    def label(self):
        if "label" in self._definition:
            label = self._definition["label"]
            if self.type == "time" and self.dimension_group:
                formatted_label = f"{label} {self.dimension_group.replace('_', ' ').title()}"
            elif self.type == "duration" and self.dimension_group:
                formatted_label = f"{self.dimension_group.replace('_', ' ').title()} {label}"
            else:
                formatted_label = label
        else:
            # Default
            label_text = self.alias().replace("_", " ")
            if len(str(label_text)) <= 4:
                formatted_label = label_text.upper()
            else:
                formatted_label = label_text.title()

        if self.label_prefix:
            return f"{self.label_prefix} {formatted_label}"
        return formatted_label

    @property
    def measure(self):
        measure = self._definition.get("measure")
        if measure:
            return self.get_field_with_view_info(measure)
        return

    @property
    def filters(self):
        filters = self._definition.get("filters")
        if filters:
            return filters
        return

    @property
    def convert_timezone(self):
        default_value = True
        if self.view.model.default_convert_tz is False or self.view.model.default_convert_timezone is False:
            default_value = False

        if "convert_timezone" in self._definition:
            convert = self._definition.get("convert_timezone", default_value)
        else:
            convert = self._definition.get("convert_tz", default_value)
        return convert

    @property
    def datatype(self):
        if "datatype" in self._definition:
            return self._definition["datatype"]
        elif self._definition["field_type"] == "dimension_group":
            return self.defaults["datatype"]
        return

    @property
    def is_merged_result(self):
        if "is_merged_result" in self._definition:
            return self._definition["is_merged_result"]
        elif self.type == "number" and self.field_type == "measure":
            # For number types, if the references have different canon_date values
            # then we need to make it a merged result.
            referenced_canon_dates = set()
            for reference in self.referenced_fields(self.sql):
                if reference.field_type == "measure" and reference.type != "cumulative":
                    referenced_canon_dates.add(reference.canon_date)

            return len(referenced_canon_dates) > 1
        return False

    @property
    def canon_date(self):
        if self._definition.get("canon_date"):
            canon_date = self._definition["canon_date"].replace("${", "").replace("}", "")
            return self._add_view_name_if_needed(canon_date)
        if self.view.default_date:
            return self._add_view_name_if_needed(self.view.default_date)
        return None

    @property
    def drill_fields(self):
        drill_fields = self._definition.get("drill_fields")
        if drill_fields:
            set_definition = {"name": "drill_fields", "fields": drill_fields, "view_name": self.view.name}
            return Set(set_definition, project=self.view.project).field_names()
        return drill_fields

    @property
    def non_additive_dimension(self):
        non_additive_dimension = self._definition.get("non_additive_dimension")
        if non_additive_dimension:
            if "." not in non_additive_dimension["name"]:
                qualified_name = f"{self.view.name}.{non_additive_dimension['name']}"
                non_additive_dimension["name"] = qualified_name
            if window_groupings := non_additive_dimension.get("window_groupings", []):
                qualified_groupings = []
                for grouping in window_groupings:
                    if "." not in grouping:
                        qualified_name = f"{self.view.name}.{grouping}"
                    else:
                        qualified_name = grouping
                    qualified_groupings.append(qualified_name)
                non_additive_dimension["window_groupings"] = qualified_groupings

        return non_additive_dimension

    @property
    def update_where_timeframe(self):
        # if this value is present we use it, otherwise we default to True
        return self._definition.get("update_where_timeframe", True)

    def cte_prefix(self, aggregated: bool = True):
        if self.type == "cumulative":
            prefix = "aggregated" if aggregated else "subquery"
            return f"{prefix}_{self.alias(with_view=True)}"
        return

    def alias(self, with_view: bool = False):
        if self.field_type == "dimension_group":
            if self.type == "time":
                alias = f"{self.name}_{self.dimension_group}"
            elif self.type == "duration":
                alias = f"{self.dimension_group}_{self.name}"
            else:
                alias = self.name
        else:
            alias = self.name
        if with_view:
            return f"{self.view.name}_{alias}"
        return alias

    def sql_query(self, query_type: str = None, functional_pk: str = None, alias_only: bool = False):
        if not query_type:
            query_type = self._derive_query_type()
        if self.type == "cumulative" and alias_only:
            return f"{self.cte_prefix()}.{self.measure.alias(with_view=True)}"
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
        # TODO add median_distinct, percentile, percentile, percentile_distinct
        sql = self.raw_sql_query(query_type, alias_only=alias_only)
        type_lookup = {
            "sum": self._sum_aggregate_sql,
            "sum_distinct": self._sum_distinct_aggregate_sql,
            "count": self._count_aggregate_sql,
            "count_distinct": self._count_distinct_aggregate_sql,
            "average": self._average_aggregate_sql,
            "average_distinct": self._average_distinct_aggregate_sql,
            "median": self._median_aggregate_sql,
            "max": self._max_aggregate_sql,
            "min": self._min_aggregate_sql,
            "number": self._number_aggregate_sql,
        }
        if self.type not in type_lookup:
            raise QueryError(
                f"Aggregate type {self.type} not supported. Supported types are: {list(type_lookup.keys())}"
            )
        return type_lookup[self.type](sql, query_type, functional_pk, alias_only)

    def strict_replaced_query(self):
        clean_sql = deepcopy(self.sql)
        fields_to_replace = self.fields_to_replace(clean_sql)
        for to_replace in fields_to_replace:
            if to_replace == "TABLE":
                clean_sql = clean_sql.replace("${TABLE}.", "")
            else:
                field = self.get_field_with_view_info(to_replace)
                if field:
                    if field.is_merged_result or field.type == "number":
                        sql_replace = "(" + field.strict_replaced_query() + ")"
                    else:
                        sql_replace = field.alias(with_view=True)
                else:
                    sql_replace = to_replace

                clean_sql = clean_sql.replace("${" + to_replace + "}", sql_replace)
        return clean_sql.strip()

    def _needs_symmetric_aggregate(self, functional_pk: MetricsLayerBase):
        if functional_pk:
            if functional_pk == Definitions.does_not_exist:
                return True
            try:
                field_pk_id = self.view.primary_key.id()
            except AttributeError:
                raise QueryError(
                    f"The primary key for the view {self.view.name} is not defined. "
                    "To use symmetric aggregates, you need to define the primary key. "
                    "Define the primary key by adding primary_key: yes to the field "
                    "that is the primary key of the table."
                )
            different_functional_pk = field_pk_id != functional_pk.id()
        else:
            different_functional_pk = False
        return different_functional_pk

    def _get_sql_distinct_key(self, sql_distinct_key: str, query_type: str, alias_only: bool):
        if self.filters:
            clean_sql_distinct_key = Filter.translate_looker_filters_to_sql(sql_distinct_key, self.filters)
        else:
            clean_sql_distinct_key = sql_distinct_key
        return self._replace_sql_query(clean_sql_distinct_key, query_type, alias_only=alias_only)

    def _count_distinct_aggregate_sql(self, sql: str, query_type: str, functional_pk: str, alias_only: bool):
        return f"COUNT(DISTINCT({sql}))"

    def _sum_aggregate_sql(self, sql: str, query_type: str, functional_pk: str, alias_only: bool):
        if (
            query_type in Definitions.symmetric_aggregates_supported_warehouses
            and self._needs_symmetric_aggregate(functional_pk)
        ):
            return self._sum_symmetric_aggregate(sql, query_type, alias_only=alias_only)
        return f"SUM({sql})"

    def _sum_distinct_aggregate_sql(self, sql: str, query_type: str, functional_pk: str, alias_only: bool):
        sql_distinct_key = self._get_sql_distinct_key(
            self.sql_distinct_key, query_type, alias_only=alias_only
        )
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
        if query_type not in Definitions.symmetric_aggregates_supported_warehouses:
            raise QueryError(
                f"Symmetric aggregates are not supported in {query_type}. "
                "Use the 'sum' type instead of 'sum_distinct'."
            )
        elif query_type in {
            Definitions.snowflake,
            Definitions.postgres,
            Definitions.duck_db,
            Definitions.redshift,
        }:
            return self._sum_symmetric_aggregate_snowflake(sql, primary_key_sql, alias_only, factor)
        elif query_type == Definitions.bigquery:
            return self._sum_symmetric_aggregate_bigquery(sql, primary_key_sql, alias_only, factor)

    def _sum_symmetric_aggregate_bigquery(
        self, sql: str, primary_key_sql: str, alias_only: bool, factor: int = 1_000_000
    ):
        if not primary_key_sql:
            raw_primary_key_sql = self.view.primary_key.sql_query(Definitions.bigquery, alias_only=alias_only)
            primary_key_sql = self._get_sql_distinct_key(
                raw_primary_key_sql, Definitions.bigquery, alias_only
            )

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
            raw_primary_key_sql = self.view.primary_key.sql_query(
                Definitions.snowflake, alias_only=alias_only
            )
            primary_key_sql = self._get_sql_distinct_key(
                raw_primary_key_sql, Definitions.snowflake, alias_only
            )

        adjusted_sum = f"(CAST(FLOOR(COALESCE({sql}, 0) * ({factor} * 1.0)) AS DECIMAL(38,0)))"

        pk_sum = f"(TO_NUMBER(MD5({primary_key_sql}), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)"  # noqa

        sum_with_pk_backout = f"SUM(DISTINCT {adjusted_sum} + {pk_sum}) - SUM(DISTINCT {pk_sum})"

        backed_out_cast = f"COALESCE(CAST(({sum_with_pk_backout}) AS DOUBLE PRECISION)"

        result = f"{backed_out_cast} / CAST(({factor}*1.0) AS DOUBLE PRECISION), 0)"
        return result

    def _count_aggregate_sql(self, sql: str, query_type: str, functional_pk: str, alias_only: bool):
        if (
            query_type in Definitions.symmetric_aggregates_supported_warehouses
            and self._needs_symmetric_aggregate(functional_pk)
        ):
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
            raw_primary_key_sql = self.view.primary_key.sql_query(query_type, alias_only=alias_only)
            primary_key_sql = self._get_sql_distinct_key(raw_primary_key_sql, query_type, alias_only)
        pk_if_not_null = f"CASE WHEN  ({sql})  IS NOT NULL THEN  {primary_key_sql}  ELSE NULL END"
        result = f"NULLIF(COUNT(DISTINCT {pk_if_not_null}), 0)"
        return result

    def _average_aggregate_sql(self, sql: str, query_type: str, functional_pk: str, alias_only: bool):
        if (
            query_type in Definitions.symmetric_aggregates_supported_warehouses
            and self._needs_symmetric_aggregate(functional_pk)
        ):
            return self._average_symmetric_aggregate(sql, query_type, alias_only=alias_only)
        return f"AVG({sql})"

    def _average_distinct_aggregate_sql(
        self, sql: str, query_type: str, functional_pk: str, alias_only: bool
    ):
        if query_type not in Definitions.symmetric_aggregates_supported_warehouses:
            raise QueryError(
                f"Symmetric aggregates are not supported in {query_type}. "
                "Use the 'average' type instead of 'average_distinct'."
            )
        sql_distinct_key = self._get_sql_distinct_key(self.sql_distinct_key, query_type, alias_only)
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

    def _median_aggregate_sql(self, sql: str, query_type: str, functional_pk: str, alias_only: bool):
        if query_type in {
            Definitions.druid,
            Definitions.postgres,
            Definitions.bigquery,
            Definitions.sql_server,
            Definitions.azure_synapse,
        }:
            raise QueryError(
                f"Median is not supported in {query_type}. Please choose another "
                f"aggregate function for the {self.id()} measure."
            )
        # Medians do not work with symmetric aggregates, so there's just the one return
        return f"MEDIAN({sql})"

    def _max_aggregate_sql(self, sql: str, query_type: str, functional_pk: str, alias_only: bool):
        # Max works natively with symmetric aggregates, so there's just the one return
        return f"MAX({sql})"

    def _min_aggregate_sql(self, sql: str, query_type: str, functional_pk: str, alias_only: bool):
        # Min works natively with symmetric aggregates, so there's just the one return
        return f"MIN({sql})"

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
                replaced = replaced.replace(proper_to_replace, f"({to_replace})")
        else:
            raise NotImplementedError(f"handle case for sql: {sql}")
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
                raise QueryError(f"Field missing required key '{k}' The field passed was {definition}")

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
        return self.dimension_group_duration_sql(sql_start, sql_end, query_type, self.dimension_group)

    @staticmethod
    def dimension_group_duration_sql(sql_start: str, sql_end: str, query_type: str, dimension_group: str):
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
            Definitions.postgres: {
                "seconds": lambda start, end: f"{meta_lookup[Definitions.postgres]['minutes'](start, end)} * 60 + DATE_PART('SECOND', AGE({end}, {start}))",  # noqa
                "minutes": lambda start, end: f"{meta_lookup[Definitions.postgres]['hours'](start, end)} * 60 + DATE_PART('MINUTE', AGE({end}, {start}))",  # noqa
                "hours": lambda start, end: f"{meta_lookup[Definitions.postgres]['days'](start, end)} * 24 + DATE_PART('HOUR', AGE({end}, {start}))",  # noqa
                "days": lambda start, end: f"DATE_PART('DAY', AGE({end}, {start}))",
                "weeks": lambda start, end: f"TRUNC(DATE_PART('DAY', AGE({end}, {start}))/7)",
                "months": lambda start, end: f"{meta_lookup[Definitions.postgres]['years'](start, end)} * 12 + (DATE_PART('month', AGE({end}, {start})))",  # noqa
                "quarters": lambda start, end: f"{meta_lookup[Definitions.postgres]['years'](start, end)} * 4 + TRUNC(DATE_PART('month', AGE({end}, {start}))/3)",  # noqa
                "years": lambda start, end: f"DATE_PART('YEAR', AGE({end}, {start}))",
            },
            Definitions.druid: {
                "seconds": lambda start, end: f"TIMESTAMPDIFF(SECOND, {start}, {end})",
                "minutes": lambda start, end: f"TIMESTAMPDIFF(MINUTE, {start}, {end})",
                "hours": lambda start, end: f"TIMESTAMPDIFF(HOUR, {start}, {end})",
                "days": lambda start, end: f"TIMESTAMPDIFF(DAY, {start}, {end})",
                "weeks": lambda start, end: f"TIMESTAMPDIFF(WEEK, {start}, {end})",
                "months": lambda start, end: f"TIMESTAMPDIFF(MONTH, {start}, {end})",
                "quarters": lambda start, end: f"TIMESTAMPDIFF(QUARTER, {start}, {end})",
                "years": lambda start, end: f"TIMESTAMPDIFF(YEAR, {start}, {end})",
            },
            Definitions.sql_server: {
                "seconds": lambda start, end: f"DATEDIFF(SECOND, {start}, {end})",
                "minutes": lambda start, end: f"DATEDIFF(MINUTE, {start}, {end})",
                "hours": lambda start, end: f"DATEDIFF(HOUR, {start}, {end})",
                "days": lambda start, end: f"DATEDIFF(DAY, {start}, {end})",
                "weeks": lambda start, end: f"DATEDIFF(WEEK, {start}, {end})",
                "months": lambda start, end: f"DATEDIFF(MONTH, {start}, {end})",
                "quarters": lambda start, end: f"DATEDIFF(QUARTER, {start}, {end})",
                "years": lambda start, end: f"DATEDIFF(YEAR, {start}, {end})",
            },
            Definitions.bigquery: {
                "seconds": lambda start, end: f"TIMESTAMP_DIFF(CAST({end} as TIMESTAMP), CAST({start} as TIMESTAMP), SECOND)",  # noqa
                "minutes": lambda start, end: f"TIMESTAMP_DIFF(CAST({end} as TIMESTAMP), CAST({start} as TIMESTAMP), MINUTE)",  # noqa
                "hours": lambda start, end: f"TIMESTAMP_DIFF(CAST({end} as TIMESTAMP), CAST({start} as TIMESTAMP), HOUR)",  # noqa
                "days": lambda start, end: f"DATE_DIFF(CAST({end} as DATE), CAST({start} as DATE), DAY)",
                "weeks": lambda start, end: f"DATE_DIFF(CAST({end} as DATE), CAST({start} as DATE), ISOWEEK)",
                "months": lambda start, end: f"DATE_DIFF(CAST({end} as DATE), CAST({start} as DATE), MONTH)",
                "quarters": lambda start, end: f"DATE_DIFF(CAST({end} as DATE), CAST({start} as DATE), QUARTER)",  # noqa
                "years": lambda start, end: f"DATE_DIFF(CAST({end} as DATE), CAST({start} as DATE), ISOYEAR)",
            },
        }
        # SQL Server and Databricks have identical syntax in this case
        meta_lookup[Definitions.databricks] = meta_lookup[Definitions.sql_server]
        # Snowflake and redshift have identical syntax in this case
        meta_lookup[Definitions.redshift] = meta_lookup[Definitions.snowflake]
        # Snowflake and duck db have identical syntax in this case
        meta_lookup[Definitions.duck_db] = meta_lookup[Definitions.snowflake]
        # Azure Synapse and SQL Server have identical syntax
        meta_lookup[Definitions.azure_synapse] = meta_lookup[Definitions.sql_server]
        try:
            return meta_lookup[query_type][dimension_group](sql_start, sql_end)
        except KeyError:
            raise QueryError(
                f"Unable to find a valid method for running "
                f"{dimension_group} with query type {query_type}"
            )

    def apply_dimension_group_time_sql(self, sql: str, query_type: str):
        meta_lookup = {
            Definitions.snowflake: {
                "raw": lambda s, qt: s,
                "time": lambda s, qt: f"CAST({s} AS TIMESTAMP)",
                "second": lambda s, qt: f"DATE_TRUNC('SECOND', {s})",
                "minute": lambda s, qt: f"DATE_TRUNC('MINUTE', {s})",
                "hour": lambda s, qt: f"DATE_TRUNC('HOUR', {s})",
                "date": lambda s, qt: f"DATE_TRUNC('DAY', {s})",
                "week": self._week_dimension_group_time_sql,
                "month": lambda s, qt: f"DATE_TRUNC('MONTH', {s})",
                "quarter": lambda s, qt: f"DATE_TRUNC('QUARTER', {s})",
                "year": lambda s, qt: f"DATE_TRUNC('YEAR', {s})",
                "week_index": lambda s, qt: f"EXTRACT(WEEK FROM {s})",
                "week_of_month": lambda s, qt: f"EXTRACT(WEEK FROM {s}) - EXTRACT(WEEK FROM DATE_TRUNC('MONTH', {s})) + 1",  # noqa
                "month_of_year_index": lambda s, qt: f"EXTRACT(MONTH FROM {s})",
                "month_of_year": lambda s, qt: f"TO_CHAR(CAST({s} AS TIMESTAMP), 'MON')",
                "quarter_of_year": lambda s, qt: f"EXTRACT(QUARTER FROM {s})",
                "hour_of_day": lambda s, qt: f"HOUR(CAST({s} AS TIMESTAMP))",
                "day_of_week": lambda s, qt: f"TO_CHAR(CAST({s} AS TIMESTAMP), 'Dy')",
                "day_of_month": lambda s, qt: f"EXTRACT(DAY FROM {s})",
                "day_of_year": lambda s, qt: f"EXTRACT(DOY FROM {s})",
            },
            Definitions.databricks: {
                "raw": lambda s, qt: s,
                "time": lambda s, qt: f"CAST({s} AS TIMESTAMP)",
                "second": lambda s, qt: f"DATE_TRUNC('SECOND', CAST({s} AS TIMESTAMP))",
                "minute": lambda s, qt: f"DATE_TRUNC('MINUTE', CAST({s} AS TIMESTAMP))",
                "hour": lambda s, qt: f"DATE_TRUNC('HOUR', CAST({s} AS TIMESTAMP))",
                "date": lambda s, qt: f"DATE_TRUNC('DAY', CAST({s} AS TIMESTAMP))",
                "week": self._week_dimension_group_time_sql,
                "month": lambda s, qt: f"DATE_TRUNC('MONTH', CAST({s} AS TIMESTAMP))",
                "quarter": lambda s, qt: f"DATE_TRUNC('QUARTER', CAST({s} AS TIMESTAMP))",
                "year": lambda s, qt: f"DATE_TRUNC('YEAR', CAST({s} AS TIMESTAMP))",
                "week_index": lambda s, qt: f"EXTRACT(WEEK FROM CAST({s} AS TIMESTAMP))",
                "week_of_month": lambda s, qt: f"EXTRACT(WEEK FROM CAST({s} AS TIMESTAMP)) - EXTRACT(WEEK FROM DATE_TRUNC('MONTH', CAST({s} AS TIMESTAMP))) + 1",  # noqa
                "month_of_year_index": lambda s, qt: f"EXTRACT(MONTH FROM CAST({s} AS TIMESTAMP))",
                "month_of_year": lambda s, qt: f"DATE_FORMAT(CAST({s} AS TIMESTAMP), 'MMM')",
                "quarter_of_year": lambda s, qt: f"EXTRACT(QUARTER FROM CAST({s} AS TIMESTAMP))",
                "hour_of_day": lambda s, qt: f"EXTRACT(HOUR FROM CAST({s} AS TIMESTAMP))",
                "day_of_week": lambda s, qt: f"DATE_FORMAT(CAST({s} AS TIMESTAMP), 'E')",
                "day_of_month": lambda s, qt: f"EXTRACT(DAY FROM CAST({s} AS TIMESTAMP))",
                "day_of_year": lambda s, qt: f"EXTRACT(DOY FROM CAST({s} AS TIMESTAMP))",
            },
            Definitions.postgres: {
                "raw": lambda s, qt: s,
                "time": lambda s, qt: f"CAST({s} AS TIMESTAMP)",
                "second": lambda s, qt: f"DATE_TRUNC('SECOND', CAST({s} AS TIMESTAMP))",
                "minute": lambda s, qt: f"DATE_TRUNC('MINUTE', CAST({s} AS TIMESTAMP))",
                "hour": lambda s, qt: f"DATE_TRUNC('HOUR', CAST({s} AS TIMESTAMP))",
                "date": lambda s, qt: f"DATE_TRUNC('DAY', CAST({s} AS TIMESTAMP))",
                "week": self._week_dimension_group_time_sql,
                "month": lambda s, qt: f"DATE_TRUNC('MONTH', CAST({s} AS TIMESTAMP))",
                "quarter": lambda s, qt: f"DATE_TRUNC('QUARTER', CAST({s} AS TIMESTAMP))",
                "year": lambda s, qt: f"DATE_TRUNC('YEAR', CAST({s} AS TIMESTAMP))",
                "week_index": lambda s, qt: f"EXTRACT(WEEK FROM CAST({s} AS TIMESTAMP))",
                "week_of_month": lambda s, qt: f"EXTRACT(WEEK FROM CAST({s} AS TIMESTAMP)) - EXTRACT(WEEK FROM DATE_TRUNC('MONTH', CAST({s} AS TIMESTAMP))) + 1",  # noqa
                "month_of_year_index": lambda s, qt: f"EXTRACT(MONTH FROM CAST({s} AS TIMESTAMP))",
                "month_of_year": lambda s, qt: f"TO_CHAR(CAST({s} AS TIMESTAMP), 'MON')",
                "quarter_of_year": lambda s, qt: f"EXTRACT(QUARTER FROM CAST({s} AS TIMESTAMP))",
                "hour_of_day": lambda s, qt: f"EXTRACT('HOUR' FROM CAST({s} AS TIMESTAMP))",
                "day_of_week": lambda s, qt: f"TO_CHAR(CAST({s} AS TIMESTAMP), 'Dy')",
                "day_of_month": lambda s, qt: f"EXTRACT('DAY' FROM CAST({s} AS TIMESTAMP))",
                "day_of_year": lambda s, qt: f"EXTRACT('DOY' FROM CAST({s} AS TIMESTAMP))",
            },
            Definitions.druid: {
                "raw": lambda s, qt: s,
                "time": lambda s, qt: f"CAST({s} AS TIMESTAMP)",
                "second": lambda s, qt: f"DATE_TRUNC('SECOND', CAST({s} AS TIMESTAMP))",
                "minute": lambda s, qt: f"DATE_TRUNC('MINUTE', CAST({s} AS TIMESTAMP))",
                "hour": lambda s, qt: f"DATE_TRUNC('HOUR', CAST({s} AS TIMESTAMP))",
                "date": lambda s, qt: f"DATE_TRUNC('DAY', CAST({s} AS TIMESTAMP))",
                "week": self._week_dimension_group_time_sql,
                "month": lambda s, qt: f"DATE_TRUNC('MONTH', CAST({s} AS TIMESTAMP))",
                "quarter": lambda s, qt: f"DATE_TRUNC('QUARTER', CAST({s} AS TIMESTAMP))",
                "year": lambda s, qt: f"DATE_TRUNC('YEAR', CAST({s} AS TIMESTAMP))",
                "week_index": lambda s, qt: f"EXTRACT(WEEK FROM CAST({s} AS TIMESTAMP))",
                "week_of_month": lambda s, qt: f"EXTRACT(WEEK FROM CAST({s} AS TIMESTAMP)) - EXTRACT(WEEK FROM DATE_TRUNC('MONTH', CAST({s} AS TIMESTAMP))) + 1",  # noqa
                "month_of_year_index": lambda s, qt: f"EXTRACT(MONTH FROM CAST({s} AS TIMESTAMP))",
                "month_of_year": lambda s, qt: f"CASE EXTRACT(MONTH FROM CAST({s} AS TIMESTAMP)) WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr' WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec' ELSE 'Invalid Month' END",  # noqa
                "quarter_of_year": lambda s, qt: f"EXTRACT(QUARTER FROM CAST({s} AS TIMESTAMP))",
                "hour_of_day": lambda s, qt: f"EXTRACT(HOUR FROM CAST({s} AS TIMESTAMP))",
                "day_of_week": lambda s, qt: f"CASE EXTRACT(DOW FROM CAST({s} AS TIMESTAMP)) WHEN 1 THEN 'Mon' WHEN 2 THEN 'Tue' WHEN 3 THEN 'Wed' WHEN 4 THEN 'Thu' WHEN 5 THEN 'Fri' WHEN 6 THEN 'Sat' WHEN 7 THEN 'Sun' ELSE 'Invalid Day' END",  # noqa
                "day_of_month": lambda s, qt: f"EXTRACT(DAY FROM CAST({s} AS TIMESTAMP))",
                "day_of_year": lambda s, qt: f"EXTRACT(DOY FROM CAST({s} AS TIMESTAMP))",
            },
            Definitions.sql_server: {
                "raw": lambda s, qt: s,
                "time": lambda s, qt: f"CAST({s} AS DATETIME)",
                "second": lambda s, qt: f"DATEADD(SECOND, DATEDIFF(SECOND, 0, CAST({s} AS DATETIME)), 0)",
                "minute": lambda s, qt: f"DATEADD(MINUTE, DATEDIFF(MINUTE, 0, CAST({s} AS DATETIME)), 0)",
                "hour": lambda s, qt: f"DATEADD(HOUR, DATEDIFF(HOUR, 0, CAST({s} AS DATETIME)), 0)",
                "date": lambda s, qt: f"CAST(CAST({s} AS DATE) AS DATETIME)",
                "week": self._week_dimension_group_time_sql,
                "month": lambda s, qt: f"DATEADD(MONTH, DATEDIFF(MONTH, 0, CAST({s} AS DATE)), 0)",
                "quarter": lambda s, qt: f"DATEADD(QUARTER, DATEDIFF(QUARTER, 0, CAST({s} AS DATE)), 0)",
                "year": lambda s, qt: f"DATEADD(YEAR, DATEDIFF(YEAR, 0, CAST({s} AS DATE)), 0)",
                "week_index": lambda s, qt: f"EXTRACT(WEEK FROM CAST({s} AS DATE))",
                "week_of_month": lambda s, qt: f"EXTRACT(WEEK FROM CAST({s} AS DATE)) - EXTRACT(WEEK FROM DATEADD(MONTH, DATEDIFF(MONTH, 0, CAST({s} AS DATE)), 0)) + 1",  # noqa
                "month_of_year_index": lambda s, qt: f"EXTRACT(MONTH FROM CAST({s} AS DATE))",
                "month_of_year": lambda s, qt: f"LEFT(DATENAME(MONTH, CAST({s} AS DATE)), 3)",
                "quarter_of_year": lambda s, qt: f"DATEPART(QUARTER, CAST({s} AS DATE))",
                "hour_of_day": lambda s, qt: f"DATEPART(HOUR, CAST({s} AS DATETIME))",
                "day_of_week": lambda s, qt: f"LEFT(DATENAME(WEEKDAY, CAST({s} AS DATE)), 3)",
                "day_of_month": lambda s, qt: f"DATEPART(DAY, CAST({s} AS DATE))",
                "day_of_year": lambda s, qt: f"DATEPART(Y, CAST({s} AS DATE))",
            },
            Definitions.bigquery: {
                "raw": lambda s, qt: s,
                "time": lambda s, qt: f"CAST({s} AS TIMESTAMP)",
                "second": lambda s, qt: f"CAST(DATETIME_TRUNC(CAST({s} AS DATETIME), SECOND) AS {self.datatype.upper()})",  # noqa
                "minute": lambda s, qt: f"CAST(DATETIME_TRUNC(CAST({s} AS DATETIME), MINUTE) AS {self.datatype.upper()})",  # noqa
                "hour": lambda s, qt: f"CAST(DATETIME_TRUNC(CAST({s} AS DATETIME), HOUR) AS {self.datatype.upper()})",  # noqa
                "date": lambda s, qt: f"CAST(DATE_TRUNC(CAST({s} AS DATE), DAY) AS {self.datatype.upper()})",
                "week": self._week_dimension_group_time_sql,
                "month": lambda s, qt: f"CAST(DATE_TRUNC(CAST({s} AS DATE), MONTH) AS {self.datatype.upper()})",  # noqa
                "quarter": lambda s, qt: f"CAST(DATE_TRUNC(CAST({s} AS DATE), QUARTER) AS {self.datatype.upper()})",  # noqa
                "year": lambda s, qt: f"CAST(DATE_TRUNC(CAST({s} AS DATE), YEAR) AS {self.datatype.upper()})",
                "week_index": lambda s, qt: f"EXTRACT(WEEK FROM {s})",
                "week_of_month": lambda s, qt: f"EXTRACT(WEEK FROM {s}) - EXTRACT(WEEK FROM DATE_TRUNC(CAST({s} AS DATE), MONTH)) + 1",  # noqa
                "month_of_year_index": lambda s, qt: f"EXTRACT(MONTH FROM {s})",
                "month_of_year": lambda s, qt: f"FORMAT_DATETIME('%B', CAST({s} as DATETIME))",
                "quarter_of_year": lambda s, qt: f"EXTRACT(QUARTER FROM {s})",
                "hour_of_day": lambda s, qt: f"CAST({s} AS STRING FORMAT 'HH24')",
                "day_of_week": lambda s, qt: f"CAST({s} AS STRING FORMAT 'DAY')",
                "day_of_month": lambda s, qt: f"EXTRACT(DAY FROM {s})",
                "day_of_year": lambda s, qt: f"EXTRACT(DAYOFYEAR FROM {s})",
            },
        }
        # Snowflake and redshift have identical syntax in this case
        meta_lookup[Definitions.redshift] = meta_lookup[Definitions.snowflake]
        # Snowflake and duck db have identical syntax in this case
        meta_lookup[Definitions.duck_db] = meta_lookup[Definitions.postgres]
        # Azure Synapse and SQL Server have identical syntax
        meta_lookup[Definitions.azure_synapse] = meta_lookup[Definitions.sql_server]
        # We alias month_name as the same thing as month_of_year to aid with looker migration
        for _, lookup in meta_lookup.items():
            lookup["month_name"] = lookup["month_of_year"]
            lookup["month_index"] = lookup["month_of_year_index"]
            lookup["week_of_year"] = lookup["week_index"]

        if self.view.project.timezone and self.convert_timezone:
            sql = self._apply_timezone_to_sql(sql, self.view.project.timezone, query_type)
        return meta_lookup[query_type][self.dimension_group](sql, query_type)

    def _apply_timezone_to_sql(self, sql: str, timezone: str, query_type: str):
        # We need the second cast here in the case you apply the timezone with
        # the dimension group 'raw' to ensure they're the same initial type post-timezone transformation
        if query_type in {Definitions.snowflake, Definitions.databricks}:
            return f"CAST(CAST(CONVERT_TIMEZONE('{timezone}', {sql}) AS TIMESTAMP_NTZ) AS {self.datatype.upper()})"  # noqa
        elif query_type == Definitions.bigquery:
            return f"CAST(DATETIME(CAST({sql} AS TIMESTAMP), '{timezone}') AS {self.datatype.upper()})"
        elif query_type == Definitions.redshift:
            return f"CAST(CAST(CONVERT_TIMEZONE('{timezone}', {sql}) AS TIMESTAMP) AS {self.datatype.upper()})"  # noqa
        elif query_type in {Definitions.postgres, Definitions.duck_db}:
            return f"CAST(CAST({sql} AS TIMESTAMP) at time zone 'utc' at time zone '{timezone}' AS {self.datatype.upper()})"  # noqa
        elif query_type in {Definitions.druid, Definitions.sql_server, Definitions.azure_synapse}:
            print(
                f"Warning: {query_type.title()} does not support timezone conversion. "
                "Timezone will be ignored."
            )
            return sql
        else:
            raise QueryError(f"Unable to apply timezone to sql for query type {query_type}")

    def _week_dimension_group_time_sql(self, sql: str, query_type: str):
        # Monday is the default date for warehouses
        week_start_day = self.view.week_start_day
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
        positioned_sql = self._week_sql_date_trunc(sql, offset, query_type)
        if query_type == Definitions.bigquery:
            positioned_sql = f"CAST({positioned_sql} AS {self.datatype.upper()})"
        return positioned_sql

    @staticmethod
    def _week_sql_date_trunc(sql, offset, query_type):
        casted = f"CAST({sql} AS DATE)"
        if query_type in {Definitions.snowflake, Definitions.redshift}:
            if offset is None:
                return f"DATE_TRUNC('WEEK', {casted})"
            return f"DATE_TRUNC('WEEK', {casted} + {offset}) - {offset}"
        elif query_type in {
            Definitions.postgres,
            Definitions.druid,
            Definitions.duck_db,
            Definitions.databricks,
        }:
            if offset is None:
                return f"DATE_TRUNC('WEEK', CAST({sql} AS TIMESTAMP))"
            return f"DATE_TRUNC('WEEK', CAST({sql} AS TIMESTAMP) + INTERVAL '{offset}' DAY) - INTERVAL '{offset}' DAY"  # noqa
        elif query_type == Definitions.bigquery:
            if offset is None:
                return f"DATE_TRUNC({casted}, WEEK)"
            return f"DATE_TRUNC({casted} + {offset}, WEEK) - {offset}"
        elif query_type in {Definitions.sql_server, Definitions.azure_synapse}:
            if offset is None:
                return f"DATEADD(WEEK, DATEDIFF(WEEK, 0, {casted}), 0)"
            return f"DATEADD(DAY, -{offset}, DATEADD(WEEK, DATEDIFF(WEEK, 0, DATEADD(DAY, {offset}, {casted})), 0))"  # noqa
        else:
            raise QueryError(f"Unable to find a valid method for running week with query type {query_type}")

    def collect_errors(self):
        warning_prefix = "Warning:"
        errors = []
        if not self.valid_name(self.name):
            errors.append(self.name_error("field", self.name))

        if self.type == "time" and "intervals" in self._definition:
            errors.append(
                f"Field {self.name} is of type time, but has property "
                "intervals when it should have property timeframes"
            )
        if self.type == "time":
            timeframes = self._definition.get("timeframes", [])
            if not timeframes:
                errors.append(
                    f"Field {self.name} is of type time and but does not have values for the timeframe "
                    f"property. Add valid timeframes (options: {VALID_TIMEFRAMES})"
                )
            for i in timeframes:
                if i not in VALID_TIMEFRAMES:
                    errors.append(
                        f"Field {self.name} is of type time and has timeframe value of '{i}' "
                        f"which is not a valid timeframes (valid timeframes are {VALID_TIMEFRAMES})"
                    )

        if self.type == "duration" and "timeframes" in self._definition:
            errors.append(
                f"Field {self.name} is of type duration, but has property "
                "timeframes when it should have property intervals"
            )
        if self.type == "duration":
            intervals = self._definition.get("intervals", [])
            for i in intervals:
                if i not in VALID_INTERVALS:
                    errors.append(
                        f"Field {self.name} is of type duration and has interval value of '{i}' "
                        f"which is not a valid interval (valid intervals are {VALID_INTERVALS})"
                    )
        if self.field_type == "measure" and not self.canon_date:
            if self.is_merged_result:
                error_text = (
                    f"Field {self.name} is a merged result metric (measure), but does not have a date "
                    "associated with it. Associate a date with the metric (measure) by setting either "
                    "the canon_date property on the measure itself or the default_date property on "
                    "the view the measure is in. Merged results are not possible without associated dates."
                )
            else:
                error_text = (
                    f"{warning_prefix} Field {self.name} is a metric (measure), but does not have a date "
                    "associated with it. Associate a date with the metric (measure) by setting either the "
                    "canon_date property on the measure itself or the default_date property on the view the "
                    "measure is in. Time periods and merged results will not be possible to use until you "
                    "define the date association"
                )
            errors.append(error_text)

        if self.field_type in {"measure", "dimension_group"} and self.type is None:
            error_text = (
                f"Field {self.name} is a {self.field_type}, but does not have a type associated "
                f"with it. You must set a type for this {self.field_type}."
            )
            errors.append(error_text)

        if self.sql and self.sql == "${" + self.name + "}":
            error_text = (
                f"Field {self.name} references itself in its 'sql' property. You need to reference "
                "a column using the ${TABLE}.myfield_name syntax or reference another dimension or measure."
            )
            errors.append(error_text)

        if self.get_referenced_sql_query(strings_only=False) is None:
            error_text = (
                f"Field {self.view.name}.{self.name} contains invalid SQL for Zenlytic. "
                "Remove any Looker parameter references from the SQL."
            )
            errors.append(error_text)

        if self.filters:
            for f in self.filters:
                if any(k not in f for k in ["field", "value"]):
                    key = "value" if "field" in f else "value"
                    error_text = f"Field {self.name} has a filter {f} that is missing the key {key}."
                    errors.append(error_text)

        if "." in str(self.view.default_date):
            view_default_date = self.view.default_date
        else:
            view_default_date = f"{self.view.name}.{self.view.default_date}"
        if self.canon_date is not None and self.canon_date != view_default_date:
            try:
                canon_date_field = self.view.project.get_field_by_name(self.canon_date)
                if canon_date_field.field_type != "dimension_group" or canon_date_field.type != "time":
                    errors.append(
                        f"Canon date {self.canon_date} is not of field_type: dimension_group and type: time in field {self.name}."  # noqa
                    )
            except (AccessDeniedOrDoesNotExistException, QueryError):
                errors.append(f"Canon date {self.canon_date} is unreachable in field {self.name}.")

        if self.value_format_name:
            if self.value_format_name not in VALID_VALUE_FORMAT_NAMES:
                errors.append(
                    f"{warning_prefix} Field {self.name} has an invalid value_format_name "
                    f"{self.value_format_name}. Valid value_format_name's are: {VALID_VALUE_FORMAT_NAMES}"
                )

        # For personal fields everything is a warning
        if self.is_personal_field:
            errors = [f"{warning_prefix} {e}" for e in errors if warning_prefix not in e]
        return errors

    def get_referenced_sql_query(self, strings_only=True):
        if self.sql and ("{%" in self.sql or self.sql == ""):
            return None

        if self.type == "cumulative":
            referenced_fields = [self.measure]

        elif self.sql_start and self.sql_end and self.type == "duration":
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
        if sql is None:
            return []

        for to_replace in self.fields_to_replace(sql):
            if to_replace != "TABLE":
                try:
                    field = self.get_field_with_view_info(to_replace)

                except AccessDeniedOrDoesNotExistException:
                    field = None
                to_replace_type = None if field is None else field.type

                if to_replace_type == "number":
                    reference_fields_raw = field.get_referenced_sql_query(strings_only=False)
                    if reference_fields_raw is not None:
                        reference_fields.extend(reference_fields_raw)
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

        if self.type == "cumulative":
            raise QueryError(
                f"You cannot call sql_query() on cumulative type field {self.id()} because cumulative "
                "queries are dependent on the 'FROM' clause to be correct and the sql_query() method "
                "only returns the aggregation of the individual metric, not the whole SQL query. "
                "To see the query, use get_sql_query() with the cumulative metric."
            )

        raise QueryError(f"Unknown type of SQL query for field {self.name}")

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
                if isinstance(sql_replace, list):
                    raise QueryError(
                        f"Field {self.name} has the wrong type. You must use the type 'number' "
                        f"if you reference other measures in your expression (like {to_replace} "
                        "referenced here)"
                    )
                clean_sql = clean_sql.replace("${" + to_replace + "}", sql_replace)
        return clean_sql.strip()

    def get_field_with_view_info(self, field: str, specified_view: str = None):
        _, view_name, field_name = self.field_name_parts(field)
        if view_name is None and specified_view is None:
            view_name = self.view.name
        elif view_name is None and specified_view:
            view_name = specified_view

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
        model = self.view.model
        if model is None:
            raise QueryError(
                f"Could not find a model in field {self.alias()} to use to detect the query type, "
                "please pass the query type explicitly using the query_type argument"
                "or pass an model name using the model_name argument"
            )
        connection_type = self.view.project.connection_lookup.get(model.connection)
        if connection_type is None:
            raise QueryError(
                f"Could not find the connection named {model.connection} "
                f"in model {model.name} to use in detecting the query type, "
                "please pass the query type explicitly using the query_type argument"
            )
        return connection_type

    def _add_view_name_if_needed(self, field_name: str):
        if "." in field_name:
            return field_name
        return f"{self.view.name}.{field_name}"

    def non_additive_alias(self):
        if self.non_additive_dimension:
            window_choice = self.non_additive_dimension["window_choice"]
            window_name = self.non_additive_dimension["name"].split(".")[-1].lower()
            return f"{self.view.name}_{window_choice}_{window_name}"
        return None

    def non_additive_cte_alias(self):
        if self.non_additive_dimension:
            window_name = self.non_additive_dimension["name"].split(".")[-1].lower()
            return f"cte_{self.name}_{window_name}"
        return None

    def is_cumulative(self):
        explicitly_cumulative = self.type == "cumulative"
        if self.sql:
            has_references = False
            for field in self.referenced_fields(self.sql):
                if not isinstance(field, str) and field.type == "cumulative":
                    has_references = True
                    break
        else:
            has_references = False
        return explicitly_cumulative or has_references

    @staticmethod
    def _name_is_not_valid_sql(name: str):
        name_is_keyword = name is not None and name.lower() in SQL_KEYWORDS
        digit_first_char = name[0] in {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
        return name_is_keyword or digit_first_char

    @functools.lru_cache(maxsize=None)
    def join_graphs(self):
        if self.view.model is None:
            raise QueryError(
                f"Could not find a model in view {self.view.name}, "
                "please pass the model or set the model_name argument in the view"
            )

        base = self.view.project.join_graph.weak_join_graph_hashes(self.view.name)

        if self.is_cumulative():
            return base

        edges = self.view.project.join_graph.merged_results_graph(self.view.model).in_edges(self.id())
        extended = [f"merged_result_{mr}" for mr, _ in edges]
        if self.is_merged_result:
            return extended
        return list(sorted(base + extended))
