from typing import Dict, List, Tuple

from pypika import Criterion, JoinType, Table
from pypika.terms import LiteralValue

from granite.core.model.base import GraniteBase
from granite.core.model.field import Field
from granite.core.model.join import Join
from granite.core.model.view import View
from granite.core.sql.pypika_types import LiteralValueCriterion
from granite.core.sql.query_design import GraniteDesign
from granite.core.sql.query_dialect import query_lookup
from granite.core.sql.query_filter import GraniteFilter

# from granite.core.sql.query_errors import ParseError


class GraniteRawQuery(GraniteBase):
    pass


class GraniteByQuery(GraniteBase):
    """ """

    def __init__(self, definition: Dict, design: GraniteDesign) -> None:
        # The Design this Query has been built for
        self.design = design
        self.query_type = self.design.query_type
        self.query_lookup = query_lookup

        # A collection of all the column and aggregate filters in the query
        # column_filters --> WHERE clauses that use columns selected in the query
        # aggregate_filters --> HAVING clauses that use aggregates computed in the query
        self.column_filters = []

        self.validate_definition(definition)
        self.parse_definition(definition)

        super().__init__(definition)

    def validate_definition(self, definition: dict):
        pass

    def parse_definition(self, definition: dict):
        # Parse and store the pivot date provided in the Query definition
        pivot_date = definition.get("today", None)

        # Parse and store the provided filters
        filters = definition.get("filters", None)

        if filters:
            for column_filter in filters.get("columns", []):
                # Generate (and automatically validate) the filter and then store it
                print(column_filter)
                mf = GraniteFilter(
                    definition=column_filter,
                    design=self.design,
                    pivot_date=pivot_date,
                    query_type=self.query_type,
                )
                self.column_filters.append(mf)

    def needs_hda(self):
        return len(self.design.joins()) > 0

    def get_query(self):
        if self.needs_hda():
            sql = self.hda_query()
        else:
            sql = self.single_table_query()
        sql += ";"
        return sql

    def single_table_query(self):
        base_query = self.query_lookup[self.query_type]

        table = self.design.find_view(self.design.base_view_name)

        base_query = base_query.from_(self.table_expression(table))

        # Add all columns in the SELECT clause
        # select = self.get_select_columns(tables=[table])
        select = []
        for field_name in self.dimensions + self.metrics:
            field = self.design.get_field(field_name)
            select.append(self.sql(field.sql_query(), alias=field.name))

        # Add all the filters
        # where = self.get_where_criterion([table])

        # Generate the query
        no_join_query = base_query.select(*select)

        # filter
        # if len(where) > 0:
        # no_join_query = no_join_query.where(Criterion.all(where))
        group_by = []
        for field_name in self.dimensions:
            field = self.design.get_field(field_name)
            group_by.append(self.sql(field.sql_query()))

        group_by_query = no_join_query.groupby(*group_by)

        return str(group_by_query)

    def hda_query(self) -> Tuple:
        """
        Build the HDA SQL query for this Query definition.

        Returns a Tuple (sql, query_attributes, aggregate_columns)
        - sql: A string with the hda_query as a SQL:1999 compatible query
        - query_attributes: Array of hashes describing the attributes in the
           final result in the same order as the one defined by the query.
           Keys included in the hash:
           {table_name, source_name, attribute_name, attribute_label, attribute_type}
        - aggregate_columns: Array of hashes describing the aggregate columns.
           Keys included in the hash: {id, label, source}
        """
        # Build the base_join table
        base_join_query, skipped_joins = self.get_join_query()

        select = self.get_select_columns(skipped_joins=skipped_joins)
        where = self.get_where_criterion()

        base_join_query = base_join_query.select(*select)

        if len(where) > 0:
            base_join_query = base_join_query.where(Criterion.all(where))

        # base_join_query = base_join_query.limit(100)
        return str(base_join_query)

    def get_select_columns(self, tables: list = None, skipped_joins=[]):
        if tables is None:
            tables = self.design.tables()

        select_with_duplicates = []
        for table in tables:
            if table.name in skipped_joins:
                continue

            for c in table.fields():
                # Get columns to select
                select_with_duplicates.extend(self.sql_from_field(c, table))

        # Add groups to select (this is a 1 if true for group cond 0 if false)
        select_with_duplicates.extend(self.groups_with_alias())

        select = self.deduplicate_select(select_with_duplicates)

        if self.previous_period != [] and self.current_period != []:
            select.append(self._sql_with_alias(self.group_sql(self.previous_period), "__previous_period"))
            select.append(self._sql_with_alias(self.group_sql(self.current_period), "__current_period"))
        return select

    def get_where_criterion(self, tables: list = None):
        if tables is None:
            tables = self.design.tables()

        where = []
        for table in tables:
            for c in table.fields():
                # Add where clause criterion
                matching_criteria = [
                    f.criterion(LiteralValue(c.sql_query())) for f in self.column_filters if f.match(c)
                ]
                where.extend(matching_criteria)
        return where

    def get_join_query(self):
        # Build the base_join table
        base_join_query = self.query_lookup[self.query_type]

        # Base table from statement
        table = self.design.find_table(self.design.base_view_name)

        base_join_query = base_join_query.from_(self.table_expression(table))
        skipped_joins = []

        # Start By building the Join
        for join in self.design.joins():
            table = self.design.find_table(join.name)

            # Create a pypika Table based on the Table's name
            db_table = self.table_expression(table)
            if isinstance(db_table, str):
                # TODO handle valid subqueries
                print(f"Skipping un-joinable {join.name}")
                skipped_joins.append(join.name)
                continue

            criteria = LiteralValueCriterion(join.sql_on)
            join_type = self.get_pypika_join_type(join)

            base_join_query = base_join_query.join(db_table, join_type).on(criteria)

        return base_join_query, skipped_joins

    def sql_from_field(self, field: Field, table: View) -> List:
        sql = field.sql_query()
        if isinstance(sql, str):
            return [self._sql_with_alias(sql, field.alias())]
        elif isinstance(sql, list):
            query = []
            for ref in sql:
                ref_field = table.get_field(ref)
                if ref_field is not None and ref_field.sql is not None:
                    query.append(self._sql_with_alias(ref_field.sql, ref_field.alias()))
            return query
        else:
            return []

    def _sql_with_alias(self, sql_text: str, alias: str):
        return LiteralValue(sql_text + f" as {alias}")

    def deduplicate_select(self, select_with_duplicates: list):
        select, field_aliases = [], set()
        for s in select_with_duplicates:
            alias = str(s).split(" as ")[-1]
            if alias not in field_aliases:
                field_aliases.add(alias)
                select.append(s)
        return select

    def table_expression(self, view: View):
        # Create a pypika Table based on the Table's name or it's derived table sql definition
        if view.derived_table:
            table_expr = f"({view.sql}) as {view.name}"
        else:
            table_expr = Table(view.sql_table_name, alias=view.name)
        return table_expr

    @staticmethod
    def sql(sql: str, alias: str = None):
        if alias:
            return LiteralValue(sql + f" as {alias}")
        return LiteralValue(sql)

    def groups_with_alias(self):
        # Groups to select (this is a 1 if true for group cond 0 if false)
        result = []
        for group in self.groups:
            group_sql = self.group_sql(group["definition"])
            result.append(self._sql_with_alias(group_sql, group["group"]["name"]))
        return result

    def group_sql(self, group_definition: list):
        criteria = []
        group_filters = [
            GraniteFilter(definition=condition, design=self.design, query_type=self.query_type)
            for condition in group_definition
        ]
        print(group_filters)
        for table in self.design.tables():
            for c in table.fields():
                # Add where clause criterion
                matching_criteria = [
                    f.criterion(LiteralValue(c.sql_query())) for f in group_filters if f.match(c)
                ]
                criteria.extend(matching_criteria)
        int_type = "INT64" if self.query_type == "BIGQUERY" else "INT"
        return f"CAST({str(Criterion.all(criteria))} AS {int_type})"

    @staticmethod
    def get_pypika_join_type(join: Join):
        if join.type == "left_outer":
            return JoinType.left
        elif join.type == "inner":
            return JoinType.inner
        elif join.type == "full_outer":
            return JoinType.outer
        return JoinType.left
