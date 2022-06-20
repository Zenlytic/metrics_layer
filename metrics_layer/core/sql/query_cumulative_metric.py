from copy import deepcopy

from pypika import JoinType, Criterion, Table

from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.model.base import AccessDeniedOrDoesNotExistException, MetricsLayerBase
from metrics_layer.core.model.filter import LiteralValueCriterion
from metrics_layer.core.sql.query_design import MetricsLayerDesign
from metrics_layer.core.sql.query_dialect import query_lookup
from metrics_layer.core.sql.query_filter import MetricsLayerFilter
from metrics_layer.core.sql.query_generator import MetricsLayerQuery

SNOWFLAKE_DATE_SPINE = (
    "select dateadd(day, seq4(), '2000-01-01') as date from table(generator(rowcount => 365*40))"
)
BIGQUERY_DATE_SPINE = "select date from unnest(generate_date_array('2000-01-01', '2040-01-01')) as date"


class CumulativeMetricsQuery(MetricsLayerBase):
    """ """

    def __init__(self, definition: dict, design: MetricsLayerDesign, suppress_warnings: bool = False) -> None:
        self.design = design
        self.query_type = self.design.query_type
        self.no_group_by = self.design.no_group_by
        self.query_lookup = query_lookup
        self.suppress_warnings = suppress_warnings

        self.date_spine_cte_name = design.date_spine_cte_name
        self.base_cte_name = design.base_cte_name

        super().__init__(definition)

    def get_query(self, semicolon: bool = True):
        self.cumulative_metrics, self.cumulative_number_metrics, self.non_cumulative_metrics = [], [], []
        for field_name in self.metrics + self.having:
            if isinstance(field_name, str):
                field = self.design.get_field(field_name)
            else:
                field = self.design.get_field(field_name["field"])

            if field.is_cumulative():
                if field.type == "number":
                    self.cumulative_number_metrics.append(field)
                    for reference in field.referenced_fields(field.sql):
                        if reference.is_cumulative():
                            self.cumulative_metrics.append(reference)
                        else:
                            self.non_cumulative_metrics.append(reference)
                else:
                    self.cumulative_metrics.append(field)
            else:
                self.non_cumulative_metrics.append(field)

        self.non_cumulative_metrics = self.design.deduplicate_fields(self.non_cumulative_metrics)
        self.cumulative_metrics = self.design.deduplicate_fields(self.cumulative_metrics)

        base_cte_query = query_lookup[self.query_type]

        date_spine_query = self.date_spine(self.query_type)

        base_cte_query = base_cte_query.with_(Table(date_spine_query), self.date_spine_cte_name)

        for cumulative_metric in self.cumulative_metrics:
            subquery, subquery_alias = self.cumulative_subquery(cumulative_metric)
            base_cte_query = base_cte_query.with_(Table(subquery), subquery_alias)
            aggregate_subquery, aggregate_alias = self.aggregate_cumulative_subquery(cumulative_metric)
            base_cte_query = base_cte_query.with_(Table(aggregate_subquery), aggregate_alias)

        if len(self.non_cumulative_metrics) > 0:
            sub_definition = deepcopy(self._definition)
            sub_definition["metrics"] = [metric.id() for metric in self.non_cumulative_metrics]
            sub_definition["having"] = []

            field_lookup = {k: v for k, v in self.design.field_lookup.items()}
            self.design.field_lookup = {k: v for k, v in field_lookup.items() if v.field_type != "measure"}
            for metric in self.non_cumulative_metrics:
                self.design.field_lookup[metric.id()] = metric
            query_generator = MetricsLayerQuery(
                sub_definition, design=self.design, suppress_warnings=self.suppress_warnings
            )
            query = query_generator.get_query(semicolon=False)
            self.design.field_lookup = field_lookup

            base_cte_query = base_cte_query.with_(Table(query), self.base_cte_name)

        if len(self.non_cumulative_metrics) > 0:
            base = Table(self.base_cte_name)
            cumulative_idx = 0
        else:
            base = Table(self.cumulative_metrics[0].cte_prefix())
            cumulative_idx = 1

        base_cte_query = base_cte_query.from_(base)

        for cumulative_metric in self.cumulative_metrics[cumulative_idx:]:
            cte_alias = cumulative_metric.cte_prefix()

            conditions = []
            for field_name in self.dimensions:
                field = self.design.get_field(field_name)
                field_alias = field.alias(with_view=True)
                conditions.append(f"{base.get_table_name()}.{field_alias}={cte_alias}.{field_alias}")

            if len(conditions) > 0:
                raw_criteria = " and ".join(conditions)
            else:
                raw_criteria = "1=1"
            criteria = LiteralValueCriterion(raw_criteria)
            base_cte_query = base_cte_query.join(Table(cte_alias), JoinType.left).on(criteria)

        select = []
        for field_name in self.dimensions + self.metrics:
            field = self.design.get_field(field_name)
            if field.type == "number" and field.is_cumulative():

                field_sql = field.sql_query(query_type=self.query_type, alias_only=True)
            elif field.is_cumulative():
                field_sql = field.measure.alias(with_view=True)
            else:
                field_sql = field.alias(with_view=True)

            if not field.is_cumulative():
                field_sql = f"{base.get_table_name()}.{field_sql}"
            elif field.type != "number":
                field_sql = f"{field.cte_prefix()}.{field_sql}"

            select.append(MetricsLayerQuery.sql(field_sql, alias=field.alias(with_view=True)))

        base_cte_query = base_cte_query.select(*select)

        if self.having:
            where = self.get_where_from_having()
            base_cte_query = base_cte_query.where(Criterion.all(where))

        sql = str(base_cte_query)
        if semicolon:
            sql += ";"
        return sql

    @staticmethod
    def date_spine(query_type: str):
        if query_type in {Definitions.snowflake, Definitions.redshift}:
            return SNOWFLAKE_DATE_SPINE
        elif query_type == Definitions.bigquery:
            return BIGQUERY_DATE_SPINE
        raise NotImplementedError(f"Database {query_type} not implemented yet")

    def cumulative_subquery(self, cumulative_metric):
        sub_definition = deepcopy(self._definition)
        cumulative_metric_cte_alias = cumulative_metric.cte_prefix(aggregated=False)
        referenced_metric = cumulative_metric.measure
        sub_definition["metrics"] = [referenced_metric.id()]
        sub_definition["having"] = []

        self.design.no_group_by = True
        field_lookup = {k: v for k, v in self.design.field_lookup.items()}
        self.design.field_lookup = {k: v for k, v in field_lookup.items() if v.field_type != "measure"}
        self.design.field_lookup[referenced_metric.id()] = referenced_metric
        query_generator = MetricsLayerQuery(
            sub_definition, design=self.design, suppress_warnings=self.suppress_warnings
        )
        query = query_generator.get_query(semicolon=False)
        self.design.no_group_by = False
        self.design.field_lookup = field_lookup

        return query, cumulative_metric_cte_alias

    def aggregate_cumulative_subquery(self, cumulative_metric):
        cte_alias = cumulative_metric.cte_prefix(aggregated=False)

        referenced_metric = cumulative_metric.measure
        date_name = referenced_metric.view.default_date
        date_field_name = f"{date_name}_date"
        try:
            self.design.get_field(f"{referenced_metric.view.name}.{date_field_name}")
        except Exception:
            raise AccessDeniedOrDoesNotExistException(
                f"Could not find date needed to aggregate cumulative metric {cumulative_metric.name}, "
                f"looking for date field named {date_field_name} in view {referenced_metric.view.name}",
                object_type="field",
                object_name=date_field_name,
            )

        from_query = self._base_query()
        from_query = from_query.from_(Table(self.date_spine_cte_name))

        less_than_now = f"{cte_alias}.{date_field_name}<={self.date_spine_cte_name}.date"

        # For a 7 day window
        # window = f"{cte_alias}.{date_field_name}>DATEADD(day, -7, {self.date_spine_cte_name}.date)"
        criteria = LiteralValueCriterion(less_than_now)
        from_query = from_query.join(Table(cte_alias), JoinType.inner).on(criteria)

        select = []
        for field_name in [referenced_metric.id()] + self.dimensions:
            field = self.design.get_field(field_name)
            field_sql = field.sql_query(query_type=self.query_type, alias_only=True)
            select.append(MetricsLayerQuery.sql(field_sql, alias=field.alias(with_view=True)))

        from_query = from_query.select(*select)

        return from_query, cumulative_metric.cte_prefix()

    # TODO un-duplicate
    def get_where_from_having(self):
        where = []
        for having_clause in self.having:
            having_clause["query_type"] = self.query_type
            f = MetricsLayerFilter(definition=having_clause, design=None, filter_type="where")
            field = self.design.get_field(having_clause["field"])
            where.append(f.criterion(field.alias(with_view=True)))
        return where

    def _base_query(self):
        return query_lookup[self.query_type]
