import sqlparse
from sqlparse.tokens import Name, Punctuation

import re
from copy import deepcopy

from pypika import JoinType
from pypika.terms import LiteralValue

from metrics_layer.core.model.base import MetricsLayerBase
from metrics_layer.core.sql.query_filter import MetricsLayerFilter
from metrics_layer.core.model.join import Join


class MetricsLayerQueryBase(MetricsLayerBase):
    def _base_query(self):
        return self.query_lookup[self.query_type]

    def get_where_from_having(self, project):
        where = []
        for having_clause in self.having:
            having_clause["query_type"] = self.query_type
            f = MetricsLayerFilter(definition=having_clause, design=None, filter_type="where")
            field = project.get_field(having_clause["field"])
            where.append(f.criterion(field.alias(with_view=True)))
        return where

    @staticmethod
    def parse_identifiers_from_clause(clause: str):
        if clause is None:
            return []
        generator = list(sqlparse.parse(clause)[0].flatten())

        field_names = []
        for i, token in enumerate(generator):
            not_already_added = i == 0 or str(generator[i - 1]) != "."
            if token.ttype == Name and not_already_added:
                field_names.append(str(token))

            if token.ttype == Punctuation and str(token) == ".":
                if generator[i - 1].ttype == Name and generator[i + 1].ttype == Name:
                    field_names[-1] += f".{str(generator[i+1])}"
        return field_names

    @staticmethod
    def get_pypika_join_type(join: Join):
        if join.type == "left_outer":
            return JoinType.left
        elif join.type == "inner":
            return JoinType.inner
        elif join.type == "full_outer":
            return JoinType.outer
        elif join.type == "cross":
            return JoinType.cross
        return JoinType.left

    @staticmethod
    def sql(sql: str, alias: str = None):
        if alias:
            return LiteralValue(sql + f" as {alias}")
        return LiteralValue(sql)

    @staticmethod
    def strip_alias(sql: str):
        stripped_sql = deepcopy(sql)
        matches = re.findall(r"(?i)\ as\ ", stripped_sql)
        if matches:
            alias = " AS "
            for match in matches:
                stripped_sql = stripped_sql.replace(match, alias)
            return alias.join(stripped_sql.split(alias)[:-1])
        return sql


class QueryKindTypes:
    merged = "MERGED"
    single = "SINGLE"
