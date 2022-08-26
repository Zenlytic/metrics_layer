from pypika import AliasedQuery, Criterion

from metrics_layer.core.sql.query_base import MetricsLayerQueryBase
from metrics_layer.core.model.filter import LiteralValueCriterion
from metrics_layer.core.sql.query_dialect import query_lookup


class MetricsLayerMergedResultsQuery(MetricsLayerQueryBase):
    """ """

    def __init__(self, definition: dict) -> None:
        self.query_lookup = query_lookup
        super().__init__(definition)

    def get_query(self, semicolon: bool = True):
        # Build the base_cte table from the referenced queries + join them with all dimensions
        base_cte_query = self.build_cte_from()

        # Add all columns in the SELECT clause
        select = self.get_select_columns()
        complete_query = base_cte_query.select(*select)

        if self.having:
            where = self.get_where_from_having(project=self.project)
            complete_query = complete_query.where(Criterion.all(where))

        sql = str(complete_query.limit(self.limit))
        if semicolon:
            sql += ";"
        return sql

    def build_cte_from(self):
        base_cte_query = self._base_query()
        for join_hash, query in self.queries_to_join.items():
            base_cte_query = base_cte_query.with_(query, join_hash)

        for i, join_hash in enumerate(self.join_hashes):
            if i == 0:
                base_cte_query = base_cte_query.from_(AliasedQuery(join_hash))
            else:
                criteria = self._build_join_criteria(self.join_hashes[0], join_hash)
                base_cte_query = base_cte_query.inner_join(AliasedQuery(join_hash)).on(criteria)

        return base_cte_query

    def _build_join_criteria(self, first_query_alias, second_query_alias):
        no_dimensions = all(len(v) == 0 for v in self.query_dimensions.values())
        # No dimensions to join on, the query results must be just one number each
        if no_dimensions:
            return LiteralValueCriterion("1=1")

        join_criteria = []
        for i in range(len(self.query_dimensions[first_query_alias])):
            first_field = self.query_dimensions[first_query_alias][i]
            second_field = self.query_dimensions[second_query_alias][i]
            first_alias_and_id = f"{first_query_alias}.{first_field.alias(with_view=True)}"
            second_alias_and_id = f"{second_query_alias}.{second_field.alias(with_view=True)}"
            join_criteria.append(f"{first_alias_and_id}={second_alias_and_id}")

        return LiteralValueCriterion(" and ".join(join_criteria))

    # Code to handle SELECT portion of query
    def get_select_columns(self):
        select = []
        existing_aliases = []
        for join_hash, field_set in sorted(self.query_metrics.items()):
            for field in field_set:
                alias = field.alias(with_view=True)
                if alias not in existing_aliases:
                    select.append(self.sql(f"{join_hash}.{alias}", alias=alias))
                    existing_aliases.append(alias)

        for join_hash, field_set in sorted(self.query_dimensions.items()):
            for field in field_set:
                alias = field.alias(with_view=True)
                if alias not in existing_aliases:
                    select.append(self.sql(f"{join_hash}.{alias}", alias=alias))
                    existing_aliases.append(alias)

        for field in self.merged_metrics:
            alias = field.alias(with_view=True)
            if alias not in existing_aliases:
                select.append(self.sql(field.strict_replaced_query(), alias=alias))
                existing_aliases.append(alias)

        return select
