from pypika import AliasedQuery, Criterion, Order
from pypika.terms import LiteralValue

from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.model.filter import LiteralValueCriterion
from metrics_layer.core.model.join import ZenlyticJoinType
from metrics_layer.core.sql.query_base import MetricsLayerQueryBase
from metrics_layer.core.sql.query_dialect import NullSorting, query_lookup


class MetricsLayerMergedQueries(MetricsLayerQueryBase):
    """Resolve the SQL query for multiple, arbitrary merged queries"""

    def __init__(self, definition: dict) -> None:
        self.query_lookup = query_lookup
        super().__init__(definition)

    def get_query(self, semicolon: bool = True):
        # Build the base_cte table from the referenced queries + join them with all dimensions
        base_cte_query = self.build_cte_from()

        # Add all columns in the SELECT clause
        select = self.get_select_columns()
        complete_query = base_cte_query.select(*select)
        if self.where:
            where = self.get_where_with_aliases(
                self.where,
                project=self.project,
                cte_alias_lookup=self.cte_alias_lookup,
                raise_if_not_in_lookup=True,
            )
            complete_query = complete_query.where(Criterion.all(where))

        if self.having:
            # These become a where because we're only dealing with aliases, post aggregation
            having = self.get_where_with_aliases(
                self.having,
                project=self.project,
                cte_alias_lookup=self.cte_alias_lookup,
                raise_if_not_in_lookup=True,
            )
            complete_query = complete_query.where(Criterion.all(having))

        if self.order_by:
            for order_clause in self.order_by:
                field = self.project.get_field(order_clause["field"])
                order_by_alias = field.alias(with_view=True)
                if order_by_alias in self.cte_alias_lookup:
                    order_by_alias = f"{self.cte_alias_lookup[order_by_alias]}.{order_by_alias}"
                else:
                    self._raise_query_error_from_cte(field.id())

                order = Order.desc if order_clause.get("sort", "asc").lower() == "desc" else Order.asc
                complete_query = complete_query.orderby(
                    LiteralValue(order_by_alias), order=order, nulls=NullSorting.last
                )

        sql = str(complete_query.limit(self.limit))
        if semicolon:
            sql += ";"
        return sql

    def build_cte_from(self):
        base_cte_query = self._base_query()
        for query in self.merged_queries:
            base_cte_query = base_cte_query.with_(query["query"], query["cte_alias"])

        base_cte_alias = self.merged_queries[0]["cte_alias"]
        base_cte_query = base_cte_query.from_(AliasedQuery(base_cte_alias))

        # We're starting on the second one because the first one is the base in the from statement,
        # and all other queries are joined to it
        for query in self.merged_queries[1:]:
            # We have to do this because Redshift doesn't support a full outer join
            # of two CTE's without dimensions using 1=1
            if self.query_type == Definitions.redshift and len(query["join_fields"]) == 0:
                base_cte_query = base_cte_query.join(AliasedQuery(query["cte_alias"])).cross()
            else:
                criteria = self._build_join_criteria(query["join_fields"], base_cte_alias, query["cte_alias"])
                zenlytic_join_type = query.get("join_type")
                if zenlytic_join_type is None:
                    zenlytic_join_type = ZenlyticJoinType.left_outer
                join_type = self.pypika_join_type_lookup(zenlytic_join_type)
                base_cte_query = base_cte_query.join(AliasedQuery(query["cte_alias"]), how=join_type).on(
                    criteria
                )

        return base_cte_query

    def _build_join_criteria(self, join_logic: list, base_query_alias: str, joined_query_alias: str):
        # Join logic is a list with {'field': field_in_current_cte, 'source_field': field_in_base_cte}
        # No dimensions to join on, the query results must be just one number each
        if len(join_logic) == 0:
            return LiteralValueCriterion("1=1")

        join_criteria = []
        for logic in join_logic:
            joined_field = self.project.get_field(logic["field"])
            base_field = self.project.get_field(logic["source_field"])
            base_alias_and_id = f"{base_query_alias}.{base_field.alias(with_view=True)}"
            joined_alias_and_id = f"{joined_query_alias}.{joined_field.alias(with_view=True)}"
            # We need to add casting for differing datatypes on dimension groups for BigQuery
            if Definitions.bigquery == self.query_type and base_field.datatype != joined_field.datatype:
                join_condition = (
                    f"CAST({base_alias_and_id} AS TIMESTAMP)=CAST({joined_alias_and_id} AS TIMESTAMP)"
                )
            else:
                join_condition = f"{base_alias_and_id}={joined_alias_and_id}"
            join_criteria.append(join_condition)

        return LiteralValueCriterion(" and ".join(join_criteria))

    # Code to handle SELECT portion of query
    def get_select_columns(self):
        self.cte_alias_lookup = {}
        select = []
        existing_aliases = []
        for query in self.merged_queries:
            # We do not want to include the join fields in the SELECT clause,
            # unless they are part of the primary (base) query
            for j in query.get("join_fields", []):
                field = self.project.get_field(j["field"])

                alias = field.alias(with_view=True)
                existing_aliases.append(alias)

            field_ids = query.get("metrics", []) + query.get("dimensions", [])
            for field_id in field_ids:
                field = self.project.get_field(field_id)
                alias = field.alias(with_view=True)

                if alias not in existing_aliases:
                    self.cte_alias_lookup[alias] = query["cte_alias"]
                    select.append(self.sql(f"{query['cte_alias']}.{alias}", alias=alias))
                    existing_aliases.append(alias)

        return select
