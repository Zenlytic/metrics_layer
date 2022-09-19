# This takes an incoming SQL statement and converts the MQL() section of it
# into our internal format, asks the main query method to resolve the sql
# for that configuration then replaces the MQL() section of the original string
# with the correct SQL

from copy import deepcopy

import sqlparse
from sqlparse.sql import Function, Identifier, IdentifierList, Statement, Where
from sqlparse.tokens import Keyword, Whitespace

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

    The syntax can also be used for event / funnel queries, as follows:

    SELECT
        *
    FROM MQL(
            total_revenue, number_of_users

            FOR events
            FUNNEL event_name = 'user_created'
            THEN event_name = 'complete_onboarding' as onboarding,
                        event_name = 'purchase' as made_a_purchase
            WITHIN 3 days
            BY region, new_vs_repeat
            WHERE region != 'West' AND new_vs_repeat <> 'New'
        ) as subquery
    """

    def __init__(self, sql: str, project, **kwargs):
        self._function_name = "MQL"
        self.sql = sql
        self.project = project
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
        funnel_steps, funnel_for, funnel_within = [], None, {}
        mode = "metrics"
        # We do this to trim off the starting and ending parenthesis
        for token in mql_statement[1:-1]:
            if mode == "for" and token.ttype != Whitespace:
                funnel_for = str(token.value).split(" ")[0].lower().strip()
                mode = "funnel"
                funnel_step = []
                continue

            is_conjunction = str(token).strip().upper() in {"AND", "OR"}
            if (token.ttype == Keyword or self._is_funnel(token)) and not is_conjunction:
                mode = self._resolve_mode(token, mode)
                if mode == "funnel" and str(token).upper() != "THEN":
                    funnel_step = []
                    continue
                elif mode == "dimensions" and str(token).upper() in {"SOURCE"}:
                    self._add_by_mode(metrics, dimensions, mode, str(token))

            if mode == "funnel":
                if str(token).upper() == "THEN":
                    funnel_steps.append(deepcopy(funnel_step))
                    funnel_step = []
                    continue
                elif "WITHIN" in str(token).upper():
                    lowered = str(token).lower()
                    funnel_step.append(str(token)[: lowered.index("within")].strip())
                    funnel_steps.append(deepcopy(funnel_step))
                    mode = "within"
                    continue
                else:
                    funnel_step.append(str(token))
                    continue

            if mode == "within" and isinstance(token, Identifier):
                n, unit = str(token).strip().split(" ")
                funnel_within = {"value": int(n), "unit": unit.lower()}
                mode = "metrics"
                continue

            if mode == "having":
                having.append(token)
                continue

            if mode == "order_by":
                order_by.append(token)
                continue

            if isinstance(token, Identifier) and not self._is_funnel(token):
                identifier = str(token)
                self._add_by_mode(metrics, dimensions, mode, identifier)

            if isinstance(token, IdentifierList):
                for id_token in token.get_identifiers():
                    identifier = str(id_token)
                    self._add_by_mode(metrics, dimensions, mode, identifier)

            if isinstance(token, Where):
                where = token

        if funnel_within != {} and len(funnel_steps) > 0:
            steps = ["".join(s).strip() for s in funnel_steps]
            funnel = {"steps": steps, "within": funnel_within}
            if funnel_for:
                funnel["view_name"] = funnel_for
        else:
            funnel = {}

        # For all of these we do [1:] to ignore the keyword at the beginning of the statement
        where_literal = self._tokens_to_sql(where[1:])
        having_literal = self._tokens_to_sql(having[1:])
        order_by_literal = self._tokens_to_sql(order_by[1:])

        resolver = SQLQueryResolver(
            metrics=metrics,
            dimensions=dimensions,
            funnel=funnel,
            where=where_literal,
            having=having_literal,
            order_by=order_by_literal,
            project=self.project,
            **self.kwargs,
        )
        return resolver.get_query(semicolon=False)

    def _resolve_mode(self, token, mode):
        keyword = str(token).upper()
        if keyword == "BY":
            mode = "dimensions"
        elif keyword == "HAVING":
            mode = "having"
        elif keyword == "ORDER BY":
            mode = "order_by"
        elif keyword == "FOR":
            mode = "for"
        elif keyword in {"FUNNEL", "THEN"}:
            mode = "funnel"
        elif keyword == "WITHIN":
            mode = "within"
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

    def _is_funnel(self, token):
        return str(token).upper().strip() == "FUNNEL"
