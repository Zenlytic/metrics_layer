# This takes an incoming SQL statement and converts the MQL() section of it
# into our internal format, asks the main query method to resolve the sql
# for that configuration then replaces the MQL() section of the original string
# with the correct SQL

from copy import deepcopy

import sqlparse
from sqlparse.sql import Function, Identifier, IdentifierList, Statement, Where
from sqlparse.tokens import Keyword

from metrics_layer.core.parse.config import MetricsLayerConfiguration
from metrics_layer.core.sql.query_errors import ParseError
from metrics_layer.core.sql.resolve import SQLQueryResolver


class MQLConverter:
    """
    Syntax here is:

    MQL(
        metric_name
        BY
        dimension
        WHERE
        condition
        HAVING
        having_condition
        ORDER BY
        metric_name
    )

    which will resolve to the SQL query that gives that result.

    The MQL feature can be used to compose SQL queries as well, as follows

    SELECT
        countries.country,
        metric_by_country.metric_name
    SELECT countries
        LEFT JOIN
            MQL(
                metric_name
                BY
                country
                WHERE
                condition
                HAVING
                having_condition
                ORDER BY
                metric_name
            ) as metric_by_country
            ON metric_by_country.country=countries.country
    """

    def __init__(self, sql: str, config: MetricsLayerConfiguration, **kwargs):
        self._function_name = "MQL"
        self.sql = sql
        self.config = config
        self.project = self.config.project
        self._connection_name = None
        if kwargs.get("connection_name"):
            self._connection_name = kwargs.get("connection_name")

        if self._connection_name:
            self.connection = self.config.get_connection(self._connection_name)
        self.kwargs = kwargs

    def get_query(self):
        converted_sql = deepcopy(self.sql)
        statement = sqlparse.parse(self.sql)[0]
        for token in statement:
            # Handle case without an alias
            if isinstance(token, Function):
                mql_as_sql = self.parse_and_resolve_mql(token)
                if mql_as_sql:
                    # Do the replace for the MQL() function with the resolved SQL
                    converted_sql = converted_sql.replace(str(token), mql_as_sql)

            # Handle error with parenthesis
            if isinstance(token, Identifier) and str(token).upper() == self._function_name:
                raise ParseError(
                    f"Expected beginning and ending parenthesis around statement beginning with "
                    f"{str(token)} in statement {self.sql}"
                )

            # Handle case with alias
            if isinstance(token, Identifier) and f"{self._function_name}(" in str(token).upper():
                # We have to index into the first token here because the MQL is the first part of the token
                # with an alias e.g. [part 1] MQL(blah by blah) [part 2] as [part 3] alias
                mql_token = token[0]
                mql_as_sql = self.parse_and_resolve_mql(mql_token)
                if mql_as_sql:
                    # Do the replace for the MQL() function with the resolved SQL
                    converted_sql = converted_sql.replace(str(mql_token), mql_as_sql)

        if converted_sql[-1] != ";":
            converted_sql += ";"
        return converted_sql

    def parse_and_resolve_mql(self, token):
        function_name = token[0]
        if str(function_name).upper() == self._function_name:
            mql_as_sql = self.resolve_mql_statement(token[1])
            wrap_parenthesis = str(token) != self.sql
            if wrap_parenthesis:
                mql_as_sql = f"({mql_as_sql})"
            return mql_as_sql
        return

    def resolve_mql_statement(self, mql_statement):
        metrics, dimensions, where, having, order_by = [], [], [], [], []
        mode = "metrics"
        # We do this to trim off the starting and ending parenthesis
        for token in mql_statement[1:-1]:
            if token.ttype == Keyword:
                mode = self._resolve_mode(token, mode)

            if mode == "having":
                having.append(token)
                continue

            if mode == "order_by":
                order_by.append(token)
                continue

            if isinstance(token, Identifier):
                identifier = str(token)
                self._add_by_mode(metrics, dimensions, mode, identifier)

            if isinstance(token, IdentifierList):
                for id_token in token.get_identifiers():
                    identifier = str(id_token)
                    self._add_by_mode(metrics, dimensions, mode, identifier)

            if isinstance(token, Where):
                where = token

        # For all of these we do [1:] to ignore the keyword at the beginning of the statement
        where_literal = self._tokens_to_sql(where[1:])
        having_literal = self._tokens_to_sql(having[1:])
        order_by_literal = self._tokens_to_sql(order_by[1:])

        resolver = SQLQueryResolver(
            metrics=metrics,
            dimensions=dimensions,
            where=where_literal,
            having=having_literal,
            order_by=order_by_literal,
            config=self.config,
            **self.kwargs,
        )
        self.connection = resolver.connection
        return resolver.get_query(semicolon=False)

    def _resolve_mode(self, token, mode):
        keyword = str(token).upper()
        if keyword == "BY":
            mode = "dimensions"
        elif keyword == "HAVING":
            mode = "having"
        elif keyword == "ORDER BY":
            mode = "order_by"
        else:
            mode = mode
        return mode

    def _add_by_mode(self, metrics: list, dimensions: list, mode: str, identifier: str):
        if mode == "metrics":
            metrics.append(identifier)
        elif mode == "dimensions":
            dimensions.append(identifier)
        else:
            raise ParseError(f"Could not parse identifier {identifier} in statement {self.sql}")

    @staticmethod
    def _tokens_to_sql(tokens: list):
        if len(tokens) == 0:
            return
        return str(Statement(tokens)).strip()
