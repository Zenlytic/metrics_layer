import functools
from copy import deepcopy

from pypika import JoinType, Criterion, Table

from metrics_layer.core.sql.query_base import MetricsLayerQueryBase
from metrics_layer.core.exceptions import AccessDeniedOrDoesNotExistException, QueryError
from metrics_layer.core.model.filter import LiteralValueCriterion, FilterInterval
from metrics_layer.core.model.field import Field
from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.sql.query_design import MetricsLayerDesign
from metrics_layer.core.sql.query_dialect import query_lookup
from metrics_layer.core.sql.query_generator import MetricsLayerQuery
from metrics_layer.core.sql.query_filter import MetricsLayerFilter


class FunnelQuery(MetricsLayerQueryBase):
    """ """

    def __init__(self, definition: dict, design: MetricsLayerDesign, suppress_warnings: bool = False) -> None:
        self.design = design
        self.query_type = self.design.query_type
        self.no_group_by = self.design.no_group_by
        self.query_lookup = query_lookup
        self.suppress_warnings = suppress_warnings

        self.step_1_time = "step_1_time"
        self.result_cte_name = "result_cte"
        self.base_cte_name = design.base_cte_name
        super().__init__(definition)

    def __hash__(self):
        return hash(self.design.project)

    def get_query(self, semicolon: bool = True, cte_only: bool = False):
        base_cte_query = self._base_query()

        query = self.get_funnel_base()
        base_cte_query = base_cte_query.with_(Table(query), self.base_cte_name)

        for i, step in enumerate(self.funnel["steps"]):
            previous_step_number = i
            step_number = i + 1

            from_query = self._base_query()
            base_table = Table(self.base_cte_name)
            from_query = from_query.from_(base_table)

            where = self.where_for_event(step, step_number, self.event_date_alias)
            if previous_step_number == 0:
                from_query = self.get_step_1_cte(from_query, base_table)
            else:
                from_query = self.get_step_n_cte(from_query, base_table, previous_step_number)

            from_query = from_query.where(Criterion.all(where))
            base_cte_query = base_cte_query.with_(Table(from_query), self._cte(step_number))

            select, group_by = self.get_select_and_group_by(step_number)

            union_base = self._base_query()
            if step_number == 1:
                union_cte = union_base.from_(Table(self._cte(step_number))).select(*select).groupby(*group_by)
            else:
                union_cte = union_cte.union_all(
                    union_base.from_(Table(self._cte(step_number))).select(*select).groupby(*group_by)
                )

        base_cte_query = base_cte_query.with_(Table(union_cte), self.result_cte_name)

        if cte_only:
            return base_cte_query

        result_table = Table(self.result_cte_name)
        base_cte_query = base_cte_query.from_(result_table).select(result_table.star)

        if self.having:
            where = self.get_where_from_having(project=self.design)
            base_cte_query = base_cte_query.where(Criterion.all(where))

        base_cte_query = base_cte_query.limit(self.limit)
        sql = str(base_cte_query)
        if semicolon:
            sql += ";"
        return sql

    def get_select_and_group_by(self, step_number: int):
        step_select = self._get_step_select(step_number)
        base_select, group_by = self._get_base_select()
        return step_select + base_select, group_by

    @functools.lru_cache(maxsize=None)
    def _get_base_select(self):
        select = []
        group_by = []
        pk = Definitions.does_not_exist
        for field_name in self.metrics + self.dimensions:
            field = self.design.get_field(field_name)
            field_sql = field.sql_query(query_type=self.query_type, functional_pk=pk, alias_only=True)
            if field.field_type != "measure":
                group_by.append(self.sql(field_sql))
            select.append(self.sql(field_sql, alias=field.alias(with_view=True)))
        return select, group_by

    def _get_step_select(self, step_number: int):
        return [
            self.sql(f"'Step {step_number}'", alias="step"),
            self.sql(f"{step_number}", alias="step_order"),
        ]

    def get_step_1_cte(self, from_query, base_table):
        return from_query.select(base_table.star, self.sql(self.event_date_alias, alias=self.step_1_time))

    def get_step_n_cte(self, from_query, base_table, previous_step_number: int):
        prev_cte = self._cte(previous_step_number)
        match_person = f"{self.base_cte_name}.{self.link_alias}={prev_cte}.{self.link_alias}"
        valid_sequence = f"{prev_cte}.{self.event_date_alias}<{self.base_cte_name}.{self.event_date_alias}"
        criteria = LiteralValueCriterion(f"{match_person} and {valid_sequence}")

        from_query = from_query.join(Table(prev_cte), JoinType.inner).on(criteria)
        step_1_time = self.sql(f"{prev_cte}.{self.step_1_time}", alias=self.step_1_time)
        return from_query.select(base_table.star, step_1_time)

    def get_funnel_base(self):
        event_date = self.get_event_date()
        self.event_date_alias = event_date.alias(with_view=True)

        event_condition_fields = self._get_event_condition_fields()

        having_metrics = list(set(f["field"] for f in self.having)) if self.having else []
        self.metrics = self.metrics + having_metrics

        base_dimensions = self.dimensions + [event_date.id()] + event_condition_fields
        join_graphs = self.join_graphs_for_query(self.metrics, base_dimensions, self.where)

        try:
            self.link_field = self.design.project.get_field_by_tag("customer", join_graphs=join_graphs)
            self.link_alias = self.link_field.alias(with_view=True)
        except AccessDeniedOrDoesNotExistException:
            raise QueryError(
                f"No link (customer) field found for query with metrics: {self.metrics}. "
                "Make sure you have added a customer tag to the view or a view that can be joined in."
            )

        dimensions = self.dimensions + [event_date.id(), self.link_field.id()] + event_condition_fields
        query = self._subquery(self.metrics, dimensions, self.where, no_group_by=True)
        return query

    def _get_event_condition_fields(self):
        fields = []
        for step in self.funnel["steps"]:
            fields.extend(self._get_fields_from_condition(step))
        return list(set(fields))

    def _get_fields_from_condition(self, condition):
        if isinstance(condition, list):
            fields = [f["field"] for f in condition if "field" in f]
        else:
            fields = self.parse_identifiers_from_clause(condition)
        return fields

    def get_event_date(self):
        if "view_name" in self.funnel:
            view = self.design.get_view(self.funnel["view_name"])
            return self.design.get_field(f"{view.name}.{view.default_date}_raw")

        dates = []
        for metric_name in self.metrics:
            metric = self.design.get_field(metric_name)
            if "." in metric.view.default_date:
                dates.append(tuple(metric.view.default_date.split(".")))
            else:
                dates.append((metric.view.name, metric.view.default_date))

        if len(list(dates)) == 1:
            date_key = f"{list(dates)[0][0]}.{list(dates)[0][-1]}_raw"
            return self.design.get_field(date_key)
        raise QueryError(f"Could not determine event date for funnel: {self._definition}")

    @staticmethod
    def _cte(step_number: int):
        return f"step_{step_number}"

    def where_for_event(self, step: list, step_number: int, event_date_alias: str):
        where = []
        if isinstance(step, list):
            for condition in step:
                where_condition = deepcopy(condition)
                where_condition["query_type"] = self.query_type
                f = MetricsLayerFilter(definition=where_condition, design=None, filter_type="where")
                if "field" in condition:
                    where_field = self.design.get_field(condition["field"])
                    where_field_sql = where_field.sql_query(query_type=self.query_type, alias_only=True)
                    reference_value = f"{self.base_cte_name}.{where_field_sql}"
                else:
                    reference_value = f"true"
                where.append(f.criterion(reference_value))
        else:
            f = MetricsLayerFilter(
                definition={"literal": step, "query_type": self.query_type},
                design=self.design,
                filter_type="where",
            )
            where.append(f.sql_query())

        if step_number > 1:
            where.append(self._within_where(event_date_alias, step_number))
        return where

    def _within_where(self, event_date_alias: str, step_number: int):
        unit = FilterInterval.plural(self.funnel["within"]["unit"])
        value = int(self.funnel["within"]["value"])
        start = f"{self._cte(step_number-1)}.{self.step_1_time}"
        end = f"{self.base_cte_name}.{event_date_alias}"

        date_diff = Field.dimension_group_duration_sql(
            start, end, query_type=self.query_type, dimension_group=unit
        )
        return LiteralValueCriterion(f"{date_diff} <= {value}")

    def _subquery(self, metrics: list, dimensions: list, where: list, no_group_by: bool):
        sub_definition = deepcopy(self._definition)
        dimensions_to_add = self._enrich_query_design(metrics, dimensions)
        sub_definition["metrics"] = sorted(metrics)
        sub_definition["dimensions"] = sorted(dimensions + dimensions_to_add)
        sub_definition["where"] = where
        sub_definition["having"] = []
        sub_definition["limit"] = None

        self.design.no_group_by = no_group_by
        query_generator = MetricsLayerQuery(
            sub_definition, design=self.design, suppress_warnings=self.suppress_warnings
        )
        query = query_generator.get_query(semicolon=False)
        self.design.no_group_by = False

        return query

    def _enrich_query_design(self, metrics: list, dimensions: list):
        dimensions_to_add = []
        for dimension_name in dimensions:
            dimension = self.design.get_field(dimension_name)
            if dimension.id() not in self.design.field_lookup:
                self.design.field_lookup[dimension.id()] = dimension

        for metric_name in metrics:
            metric = self.design.get_field(metric_name)
            if metric.view.primary_key.id() not in self.design.field_lookup:
                self.design.field_lookup[metric.view.primary_key.id()] = metric.view.primary_key
                dimensions_to_add.append(metric.view.primary_key.id())

            if metric.id() not in self.design.field_lookup:
                self.design.field_lookup[metric.id()] = metric

        return dimensions_to_add

    def join_graphs_for_query(self, metrics, dimensions, where):
        where_fields = self._get_fields_from_condition(where)
        fields = metrics + dimensions + where_fields

        all_graphs = []
        for f in fields:
            field = self.design.project.get_field(f)
            all_graphs.append(set(jg for jg in field.join_graphs() if "merged_result" not in jg))

        remaining_join_graphs = set.intersection(*all_graphs)
        return tuple(remaining_join_graphs)
