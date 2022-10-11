from typing import Dict, List

from pypika import Criterion, Order, Table
from pypika.terms import LiteralValue

from metrics_layer.core.exceptions import QueryError
from metrics_layer.core.sql.query_base import MetricsLayerQueryBase
from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.model.field import Field
from metrics_layer.core.model.view import View
from metrics_layer.core.model.filter import LiteralValueCriterion
from metrics_layer.core.sql.query_design import MetricsLayerDesign
from metrics_layer.core.sql.query_dialect import query_lookup
from metrics_layer.core.sql.query_errors import ArgumentError
from metrics_layer.core.sql.query_filter import MetricsLayerFilter


class MetricsLayerQuery(MetricsLayerQueryBase):
    """ """

    def __init__(self, definition: Dict, design: MetricsLayerDesign, suppress_warnings: bool = False) -> None:
        # The Design this Query has been built for
        self.design = design
        self.query_type = self.design.query_type
        self.no_group_by = self.design.no_group_by
        self.query_lookup = query_lookup
        self.suppress_warnings = suppress_warnings

        # A collection of all the column and aggregate filters in the query + order by
        self.where_filters = []
        self.having_filters = []
        self.having_group_by_filters = []
        self.funnel_filters = []
        self.order_by_args = []

        self.parse_definition(definition)

        super().__init__(definition)

    def parse_definition(self, definition: dict):
        # Parse and store the provided filters
        where = definition.get("where", None)
        having = definition.get("having", None)
        order_by = definition.get("order_by", None)

        access_filter_literal, _ = self.design.get_access_filter()
        if where or access_filter_literal:
            wheres = self._parse_filter_object(where, "where", access_filter=access_filter_literal)
            self.where_filters.extend([f for f in wheres if not f.is_group_by and not f.is_funnel])
            self.funnel_filters.extend([f for f in wheres if f.is_funnel])
            if len(self.funnel_filters) > 1:
                raise QueryError("Only one funnel filter is allowed per query")
            self.having_group_by_filters.extend([f for f in wheres if f.is_group_by])

        if having and self.no_group_by:
            raise ArgumentError(
                """You cannot include the 'having' argument with the table's primary key
                as a dimension, there is no group by statement in this case, and no having can be applied"""
            )

        if having:
            having_filters = self._parse_filter_object(having, "having")
            self.having_group_by_filters.extend([f for f in having_filters if f.is_group_by])
            self.having_filters.extend([f for f in having_filters if not f.is_group_by])

        if order_by and self.no_group_by:
            raise ArgumentError(
                """You cannot include the 'order_by' argument with the table's primary key
                as a dimension, metrics that reference multiple values do not exist by themselves
                and cannot be referenced in the query"""
            )

        if order_by:
            self.order_by_args.extend(self._parse_order_by_object(order_by))
        elif self.query_type in {Definitions.snowflake, Definitions.redshift}:
            self.order_by_args.append({"field": "__DEFAULT__"})

    def _parse_filter_object(self, filter_object, filter_type: str, access_filter: str = None):
        results = []
        extra_kwargs = dict(filter_type=filter_type, design=self.design)

        if access_filter:
            filter_literal = MetricsLayerFilter(definition={"literal": access_filter}, **extra_kwargs)
            results.append(filter_literal)

        # Handle literal filter
        if isinstance(filter_object, str):
            filter_literal = MetricsLayerFilter(definition={"literal": filter_object}, **extra_kwargs)
            results.append(filter_literal)

        # Handle JSON filter
        if isinstance(filter_object, list):
            for filter_dict in filter_object:
                # Generate (and automatically validate) the filter and then store it
                f = MetricsLayerFilter(definition=filter_dict, **extra_kwargs)
                results.append(f)
        return results

    def _parse_order_by_object(self, order_by):
        results = []
        # Handle literal order by
        if isinstance(order_by, str):
            for order_clause in order_by.split(","):
                if "desc" in order_clause.lower():
                    field_reference = order_clause.lower().replace("desc", "").strip()
                    results.append({"field": field_reference, "sort": "desc"})
                else:
                    field_reference = order_clause.lower().replace("asc", "").strip()
                    results.append({"field": field_reference, "sort": "asc"})

        # Handle JSON order_by
        if isinstance(order_by, list):
            for order_clause in order_by:
                results.append({**order_clause, "sort": order_clause.get("sort", "asc").lower()})

        return results

    def needs_join(self):
        return len(self.design.joins()) > 0

    def get_query(self, semicolon: bool = True):
        if self.funnel_filters:
            funnel_filter = self.funnel_filters[0]
            base_query = funnel_filter.query_class.get_query(cte_only=True)
        else:
            base_query = self._base_query()

        # Build the base_join table if a join is needed otherwise use a single table
        if self.needs_join():
            base_query = self.get_join_query_from(base_query)
        else:
            base_query = self.get_single_table_query_from(base_query)

        # Add all columns in the SELECT clause
        select = self.get_select_columns()
        base_query = base_query.select(*select)

        # Apply the where filters
        if self.where_filters:
            where = [f.sql_query() for f in self.where_filters]
            base_query = base_query.where(Criterion.all(where))

        # Group by
        if not self.no_group_by:
            group_by = self.get_group_by_columns()
            base_query = base_query.groupby(*group_by)

        # Apply the having filters
        if self.having_filters and not self.no_group_by:
            having = [f.sql_query() for f in self.having_filters]
            base_query = base_query.having(Criterion.all(having))

        if self.having_group_by_filters:
            group_by_where = []
            for i, f in enumerate(sorted(self.having_group_by_filters)):
                cte_alias = f"filter_subquery_{i}"
                cte_query = f.cte(query_class=MetricsLayerQuery, design_class=MetricsLayerDesign)
                base_query = base_query.with_(Table(cte_query), cte_alias)
                group_by_where.append(
                    f.isin_sql_query(cte_alias=cte_alias, field_name=f.group_by, query_generator=self)
                )
            base_query = base_query.where(Criterion.all(group_by_where))

        if self.funnel_filters:
            cte_alias = "link_filter_subquery"
            funnel_filter = self.funnel_filters[0]
            cte_query = funnel_filter.funnel_cte()
            base_query = base_query.with_(Table(cte_query), cte_alias)
            link_field = funnel_filter.query_class.link_field.id()
            where_sql = funnel_filter.isin_sql_query(
                cte_alias=cte_alias, field_name=link_field, query_generator=self
            )
            base_query = base_query.where(where_sql)

        # Handle order by
        if self.order_by_args and not self.no_group_by:
            for arg in self.order_by_args:
                # If the order isn't specified, then we default to the first measure or dim if no measure
                if arg["field"] == "__DEFAULT__":
                    all_fields = self.metrics + self.dimensions
                    first_field = self.design.get_field(all_fields[0])
                    arg["sort"] = "desc" if first_field.field_type == "measure" else "asc"
                    arg["field"] = first_field.alias(with_view=True)
                else:
                    field = self.design.get_field(arg["field"])
                    arg["field"] = field.alias(with_view=True)
                order = Order.desc if arg["sort"] == "desc" else Order.asc
                base_query = base_query.orderby(LiteralValue(arg["field"]), order=order)

        completed_query = base_query.limit(self.limit)
        if self.return_pypika_query:
            return completed_query

        sql = str(completed_query)
        if semicolon:
            sql += ";"
        return sql

    # Code to handle SELECT portion of query
    def get_select_columns(self):
        if self.no_group_by:
            select = self._get_no_group_by_select_columns()
        else:
            select = self._get_group_by_select_columns()

        if self.select_raw_sql:
            select.extend([self.sql(clause) for clause in self.select_raw_sql])
        return select

    def _get_group_by_select_columns(self):
        select = []
        for field_name in self.dimensions + self.metrics:
            field = self.design.get_field(field_name)
            select.append(self.get_sql(field, alias=field.alias(with_view=True), use_symmetric=True))
        return select

    def _get_no_group_by_select_columns(self):
        select_with_duplicates = []
        for field_name in self.dimensions + self.metrics:
            field = self.design.get_field(field_name)
            select_with_duplicates.extend(self._sql_from_field_no_group_by(field))

        select = self._deduplicate_select(select_with_duplicates)
        return select

    def _sql_from_field_no_group_by(self, field: Field) -> List:
        sql = field.raw_sql_query(self.query_type)

        if isinstance(sql, str):
            return [self.sql(sql, alias=field.alias(with_view=True))]

        # This handles the special case where the measure is made up of multiple references
        elif isinstance(sql, list):
            query = []
            for reference in sorted(sql):
                referenced_field = self.design.get_field(reference)
                if referenced_field is not None:
                    query.extend(self._sql_from_field_no_group_by(referenced_field))
            return query

        else:
            return []

    def _deduplicate_select(self, select_with_duplicates: list):
        select, field_aliases = [], set()
        for s in select_with_duplicates:
            alias = str(s).split(" as ")[-1]
            if alias not in field_aliases:
                field_aliases.add(alias)
                select.append(s)
        return select

    # Code to handle the FROM portion of the query
    def get_join_query_from(self, base_join_query):
        # Base table from statement
        table = self.design.get_view(self.design.base_view_name)

        base_join_query = base_join_query.from_(self._table_expression(table))

        # Start By building the Join
        for join in self.design.joins():
            table = self.design.get_view(join.join_view_name)

            # Create a pypika Table based on the Table's name
            db_table = self._table_expression(table)

            criteria = LiteralValueCriterion(join.replaced_sql_on(self.query_type))
            join_type = self.get_pypika_join_type(join)

            if join.type == "cross":
                base_join_query = base_join_query.join(db_table, join_type).cross()
            else:
                base_join_query = base_join_query.join(db_table, join_type).on(criteria)

        return base_join_query

    def get_single_table_query_from(self, base_query):
        table = self.design.get_view(self.design.base_view_name)
        base_query = base_query.from_(self._table_expression(table))

        return base_query

    def _table_expression(self, view: View):
        # Create a pypika Table based on the Table's name or it's derived table sql definition
        if view.derived_table:
            derived_sql = view.derived_table["sql"]
            table_expr = Table(f"({derived_sql}) as {view.name}")
        else:
            table_expr = Table(view.sql_table_name, alias=view.name)
        return table_expr

    # Code for the GROUP BY part of the query
    def get_group_by_columns(self):
        group_by = []
        for field_name in self.dimensions:
            field = self.design.get_field(field_name)
            group_by.append(self.get_sql(field))

        if self.select_raw_sql:
            group_by.extend([self.sql(self.strip_alias(clause)) for clause in self.select_raw_sql])
        return group_by

    # Code for formatting values
    def get_sql(self, field, alias: str = None, use_symmetric: bool = False):
        if use_symmetric:
            query = field.sql_query(query_type=self.query_type, functional_pk=self.design.functional_pk())
        else:
            query = field.sql_query(query_type=self.query_type)
        return self.sql(query, alias)
