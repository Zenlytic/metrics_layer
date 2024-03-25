from collections import defaultdict

from pypika import AliasedQuery, Criterion

from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.model.filter import LiteralValueCriterion
from metrics_layer.core.sql.query_base import MetricsLayerQueryBase
from metrics_layer.core.sql.query_dialect import if_null_lookup, query_lookup


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
                no_dimensions = all(len(v) == 0 for v in self.query_dimensions.values())
                # We have to do this because Redshift doesn't support a full outer join
                # of two CTE's without dimensions using 1=1
                if self.query_type == Definitions.redshift and no_dimensions:
                    base_cte_query = base_cte_query.join(AliasedQuery(join_hash)).cross()
                else:
                    criteria = self._build_join_criteria(self.join_hashes[0], join_hash, no_dimensions)
                    base_cte_query = base_cte_query.outer_join(AliasedQuery(join_hash)).on(criteria)

        return base_cte_query

    def _build_join_criteria(self, first_query_alias, second_query_alias, no_dimensions: bool):
        # No dimensions to join on, the query results must be just one number each
        if no_dimensions:
            return LiteralValueCriterion("1=1")

        join_criteria = []
        for i in range(len(self.query_dimensions[first_query_alias])):
            first_field = self.query_dimensions[first_query_alias][i]
            second_field = self.query_dimensions[second_query_alias][i]
            first_alias_and_id = f"{first_query_alias}.{first_field.alias(with_view=True)}"
            second_alias_and_id = f"{second_query_alias}.{second_field.alias(with_view=True)}"
            # We need to add casting for differing datatypes on dimension groups for BigQuery
            if Definitions.bigquery == self.query_type and first_field.datatype != second_field.datatype:
                join_logic = (
                    f"CAST({first_alias_and_id} AS TIMESTAMP)=CAST({second_alias_and_id} AS TIMESTAMP)"
                )
            else:
                join_logic = f"{first_alias_and_id}={second_alias_and_id}"
            join_criteria.append(join_logic)

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

        # Map the dimensions to their counterparts (if present) for the "if null" clauses
        mapping_lookup = defaultdict(list)
        for _, fields in self.query_dimensions.items():
            for field in fields:
                field_key = f"{field.view.name}.{field.name}"
                if field.dimension_group:
                    field_key_dim_group = f"{field_key}_{field.dimension_group}"
                else:
                    field_key_dim_group = field_key
                for mapped_field in self.mapping_lookup.get(field_key, []):
                    mapped_field_key = mapped_field["field"]
                    if field.dimension_group:
                        mapped_field_key += f"_{field.dimension_group}"
                    mapping_lookup[field_key_dim_group].append(
                        {"cte": mapped_field["cte"], "field": mapped_field_key}
                    )

        dimension_sql = {}
        all_dimension_ids = [field.id() for fields in self.query_dimensions.values() for field in fields]
        for join_hash, field_set in sorted(self.query_dimensions.items()):
            for field in field_set:
                alias = field.alias(with_view=True)
                if alias not in dimension_sql:
                    dimension_sql[alias] = f"{join_hash}.{alias}"
                else:
                    if_null_func = if_null_lookup[self.query_type]
                    dimension_sql[alias] = f"{if_null_func}({dimension_sql[alias]}, {join_hash}.{alias})"

                if field.id() in mapping_lookup:
                    present_fields = [
                        f for f in mapping_lookup[field.id()] if f["field"] in all_dimension_ids
                    ]
                    if_null_func = if_null_lookup[self.query_type]
                    nested_sql = self.nested_if_null(present_fields, if_null_func)
                    dimension_sql[alias] = f"{if_null_func}({join_hash}.{alias}, {nested_sql})"

        for alias, sql in dimension_sql.items():
            select.append(self.sql(sql, alias=alias))
            existing_aliases.append(alias)

        for field in self.merged_metrics:
            alias = field.alias(with_view=True)
            if alias not in existing_aliases:
                select.append(self.sql(field.strict_replaced_query(), alias=alias))
                existing_aliases.append(alias)

        return select

    @staticmethod
    def nested_if_null(aliases, if_null_func):
        first_alias = aliases[0]["cte"] + "." + aliases[0]["field"].replace(".", "_")
        if len(aliases) == 1:
            return first_alias
        else:
            return f"{if_null_func}({first_alias}, {MetricsLayerMergedResultsQuery.nested_if_null(aliases[1:], if_null_func)})"  # noqa
