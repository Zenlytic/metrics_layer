from enum import Enum
from typing import Any, Optional

from pypika import Field, Query, Table
from pypika.dialects import MSSQLQueryBuilder, PostgreSQLQueryBuilder, QueryBuilder
from pypika.enums import Dialects
from pypika.utils import builder, format_quotes

from metrics_layer.core.model.definitions import Definitions

PostgreSQLQueryBuilder.ALIAS_QUOTE_CHAR = None
PostgreSQLQueryBuilder.QUOTE_CHAR = None


class NullSorting(Enum):
    first = "FIRST"
    last = "LAST"


class QueryBuilderWithOrderByNullsOption(QueryBuilder):
    @builder
    def replace_table(self, current_table: Optional[Table], new_table: Optional[Table]) -> "QueryBuilder":
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across
        queries.

        :param current_table:
            The table instance to be replaces.
        :param new_table:
            The table instance to replace with.
        :return:
            A copy of the query with the tables replaced.
        """
        self._from = [new_table if table == current_table else table for table in self._from]
        self._insert_table = new_table if self._insert_table == current_table else self._insert_table
        self._update_table = new_table if self._update_table == current_table else self._update_table

        self._with = [alias_query.replace_table(current_table, new_table) for alias_query in self._with]
        self._selects = [select.replace_table(current_table, new_table) for select in self._selects]
        self._columns = [column.replace_table(current_table, new_table) for column in self._columns]
        self._values = [
            [value.replace_table(current_table, new_table) for value in value_list]
            for value_list in self._values
        ]

        self._wheres = self._wheres.replace_table(current_table, new_table) if self._wheres else None
        self._prewheres = self._prewheres.replace_table(current_table, new_table) if self._prewheres else None
        self._groupbys = [groupby.replace_table(current_table, new_table) for groupby in self._groupbys]
        self._havings = self._havings.replace_table(current_table, new_table) if self._havings else None
        # Adding the slot for nulls first/last is the only change here
        self._orderbys = [
            (orderby[0].replace_table(current_table, new_table), orderby[1], orderby[2])
            for orderby in self._orderbys
        ]
        self._joins = [join.replace_table(current_table, new_table) for join in self._joins]

        if current_table in self._select_star_tables:
            self._select_star_tables.remove(current_table)
            self._select_star_tables.add(new_table)

    @builder
    def orderby(self, *fields: Any, **kwargs: Any) -> "QueryBuilder":
        for field in fields:
            field = Field(field, table=self._from[0]) if isinstance(field, str) else self.wrap_constant(field)

            self._orderbys.append((field, kwargs.get("order"), kwargs.get("nulls")))

    def _orderby_sql(
        self,
        quote_char: Optional[str] = None,
        alias_quote_char: Optional[str] = None,
        orderby_alias: bool = True,
        **kwargs: Any,
    ) -> str:
        """
        Produces the ORDER BY part of the query.  This is a list of fields and possibly their
        directionality, ASC or DESC and null sorting option (FIRST or LAST).
        The clauses are stored in the query under self._orderbys as a list of tuples
        containing the field, directionality (which can be None),
        and null sorting option (which can be None).

        If an order by field is used in the select clause,
        determined by a matching, and the orderby_alias
        is set True then the ORDER BY clause will use
        the alias, otherwise the field will be rendered as SQL.
        """
        clauses = []
        selected_aliases = {s.alias for s in self._selects}
        for field, directionality, nulls in self._orderbys:
            term = (
                format_quotes(field.alias, alias_quote_char or quote_char)
                if orderby_alias and field.alias and field.alias in selected_aliases
                else field.get_sql(quote_char=quote_char, alias_quote_char=alias_quote_char, **kwargs)
            )

            if directionality is not None:
                orient = f" {directionality.value}"
            else:
                orient = ""

            if nulls is not None:
                null_sorting = f" NULLS {nulls.value}"
            else:
                null_sorting = ""
            clauses.append(f"{term}{orient}{null_sorting}")

        return " ORDER BY {orderby}".format(orderby=",".join(clauses))


class SnowflakeQuery(Query):
    """
    Defines a query class for use with Snowflake.
    """

    @classmethod
    def _builder(cls, **kwargs) -> "SnowflakeQueryBuilderWithOrderByNullsOption":
        return SnowflakeQueryBuilderWithOrderByNullsOption(**kwargs)


class SnowflakeQueryBuilderWithOrderByNullsOption(QueryBuilderWithOrderByNullsOption):
    QUOTE_CHAR = None
    ALIAS_QUOTE_CHAR = None
    QUERY_ALIAS_QUOTE_CHAR = ""
    QUERY_CLS = SnowflakeQuery

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dialect=Dialects.SNOWFLAKE, **kwargs)


class PostgresQuery(Query):
    """
    Defines a query class for use with Snowflake.
    """

    @classmethod
    def _builder(cls, **kwargs) -> PostgreSQLQueryBuilder:
        return PostgreSQLQueryBuilder(**kwargs)


class PostgresQueryWithOrderByNullsOption(Query):
    """
    Defines a query class for use with Snowflake.
    """

    @classmethod
    def _builder(cls, **kwargs) -> "PostgreSQLQueryBuilderWithOrderByNullsOption":
        return PostgreSQLQueryBuilderWithOrderByNullsOption(**kwargs)


class PostgreSQLQueryBuilderWithOrderByNullsOption(
    PostgreSQLQueryBuilder, QueryBuilderWithOrderByNullsOption
):
    QUERY_CLS = PostgresQueryWithOrderByNullsOption


class RedshiftQuery(Query):
    """
    Defines a query class for use with Amazon Redshift.
    """

    @classmethod
    def _builder(cls, **kwargs) -> "RedShiftQueryBuilderWithOrderByNullsOption":
        return RedShiftQueryBuilderWithOrderByNullsOption(dialect=Dialects.REDSHIFT, **kwargs)


class RedShiftQueryBuilderWithOrderByNullsOption(QueryBuilderWithOrderByNullsOption):
    ALIAS_QUOTE_CHAR = None
    QUOTE_CHAR = None
    QUERY_CLS = RedshiftQuery


class MSSQLQueryBuilderCorrectLimit(MSSQLQueryBuilder):
    QUOTE_CHAR = None

    @builder
    def limit(self, limit: int):
        self._top = limit


class MSSSQLQuery(Query):
    """
    Defines a query class for use with Microsoft SQL Server (and other T-SQL flavors).
    """

    @classmethod
    def _builder(cls, **kwargs) -> MSSQLQueryBuilderCorrectLimit:
        return MSSQLQueryBuilderCorrectLimit(**kwargs)


query_lookup = {
    Definitions.snowflake: SnowflakeQuery,
    Definitions.bigquery: SnowflakeQuery,  # In terms of quoting, these are the same
    Definitions.redshift: RedshiftQuery,
    Definitions.postgres: PostgresQueryWithOrderByNullsOption,
    Definitions.druid: PostgresQuery,  # druid core query logic is postgres compatible, minus null sorting
    Definitions.duck_db: PostgresQueryWithOrderByNullsOption,  # duck db core query logic = postgres
    Definitions.databricks: PostgresQueryWithOrderByNullsOption,  # databricks core query logic = postgres
    Definitions.trino: PostgresQueryWithOrderByNullsOption,  # trino core query logic = postgres
    Definitions.sql_server: MSSSQLQuery,
    Definitions.azure_synapse: MSSSQLQuery,  # Azure Synapse is a T-SQL flavor
}

if_null_lookup = {
    Definitions.snowflake: "ifnull",
    Definitions.bigquery: "ifnull",
    Definitions.redshift: "nvl",
    Definitions.postgres: "coalesce",
    Definitions.databricks: "coalesce",
    Definitions.druid: "nvl",
    Definitions.duck_db: "coalesce",
    Definitions.trino: "coalesce",
    Definitions.sql_server: "isnull",
    Definitions.azure_synapse: "isnull",
}
