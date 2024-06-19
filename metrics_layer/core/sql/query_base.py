import re
from copy import deepcopy

import sqlparse
from pypika import JoinType
from pypika.terms import LiteralValue
from sqlparse.tokens import Name, Punctuation

from metrics_layer.core.exceptions import QueryError
from metrics_layer.core.model.base import MetricsLayerBase
from metrics_layer.core.model.join import Join, ZenlyticJoinType
from metrics_layer.core.sql.query_filter import MetricsLayerFilter


class MetricsLayerQueryBase(MetricsLayerBase):
    def _base_query(self):
        return self.query_lookup[self.query_type]

    def get_where_with_aliases(
        self, filters: list, project, cte_alias_lookup: dict = {}, raise_if_not_in_lookup: bool = False
    ):
        where = []
        for filter_clause in filters:
            filter_clause["query_type"] = self.query_type
            f = MetricsLayerFilter(definition=filter_clause, design=None, filter_type="where")
            field = project.get_field(filter_clause["field"])
            field_alias = field.alias(with_view=True)
            if field_alias in cte_alias_lookup:
                field_alias = f"{cte_alias_lookup[field_alias]}.{field_alias}"
            elif raise_if_not_in_lookup:
                self._raise_query_error_from_cte(field.id(capitalize_alias=True))
            where.append(f.criterion(field_alias))
        return where

    @staticmethod
    def _raise_query_error_from_cte(field_name: str):
        raise QueryError(
            f"Field {field_name} is not present in either source query, so it"
            " cannot be applied as a filter. Please add it to one of the source queries."
        )

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
        return MetricsLayerQueryBase.pypika_join_type_lookup(join.join_type)

    @staticmethod
    def pypika_join_type_lookup(join_type: str):
        if join_type == ZenlyticJoinType.left_outer:
            return JoinType.left
        elif join_type == ZenlyticJoinType.inner:
            return JoinType.inner
        elif join_type == ZenlyticJoinType.full_outer:
            return JoinType.outer
        elif join_type == ZenlyticJoinType.cross:
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
