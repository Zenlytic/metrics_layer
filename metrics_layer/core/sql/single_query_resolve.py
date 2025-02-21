from metrics_layer.core.exceptions import QueryError
from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.sql.query_cumulative_metric import CumulativeMetricsQuery
from metrics_layer.core.sql.query_design import MetricsLayerDesign
from metrics_layer.core.sql.query_funnel import FunnelQuery
from metrics_layer.core.sql.query_generator import MetricsLayerQuery
from metrics_layer.core.utils import flatten_filters


class SingleSQLQueryResolver:
    def __init__(
        self,
        metrics: list,
        dimensions: list = [],
        funnel: dict = {},
        where: str = None,  # Either a list of json or a string
        having: str = None,  # Either a list of json or a string
        order_by: str = None,  # Either a list of json or a string
        model=None,
        project=None,
        **kwargs,
    ):
        self.field_lookup = {}
        self.no_group_by = False
        self.has_cumulative_metric = False
        self.verbose = kwargs.get("verbose", False)
        self.select_raw_sql = kwargs.get("select_raw_sql", [])
        self.explore_name = kwargs.get("explore_name")
        self.suppress_warnings = kwargs.get("suppress_warnings", False)
        self.limit = kwargs.get("limit")
        self.return_pypika_query = kwargs.get("return_pypika_query")
        self.force_group_by = kwargs.get("force_group_by", False)
        self.project = project
        self.metrics = metrics
        self.dimensions = dimensions
        self.funnel, self.is_funnel_query = self.parse_funnel(funnel)
        self.model = model
        self.parse_field_names(where, having, order_by)
        self.nesting_depth = kwargs.get("nesting_depth", 0)
        self.query_type = kwargs.get("query_type")
        if self.query_type is None:
            raise QueryError(
                "Could not determine query_type. Please have connection information for "
                "your warehouse in the configuration or explicitly pass the "
                "'query_type' argument to this function"
            )
        self.parse_input()

    def get_query(self, semicolon: bool = True):
        self.design = MetricsLayerDesign(
            no_group_by=self.no_group_by,
            query_type=self.query_type,
            field_lookup=self.field_lookup,
            model=self.model,
            project=self.project,
        )

        query_definition = {
            "metrics": self.metrics,
            "dimensions": self.dimensions,
            "funnel": self.funnel,
            "where": self.parse_where(self.where),
            "having": self.parse_having(self.having),
            "order_by": self.order_by,
            "select_raw_sql": self.select_raw_sql,
            "limit": self.limit,
            "return_pypika_query": self.return_pypika_query,
            "nesting_depth": self.nesting_depth,
        }
        if self.has_cumulative_metric and self.is_funnel_query:
            raise QueryError("Cumulative metrics cannot be used with funnel queries")

        elif self.has_cumulative_metric:
            query_generator = CumulativeMetricsQuery(
                query_definition, design=self.design, suppress_warnings=self.suppress_warnings
            )

        elif self.is_funnel_query:
            query_generator = FunnelQuery(
                query_definition, design=self.design, suppress_warnings=self.suppress_warnings
            )

        else:
            query_generator = MetricsLayerQuery(
                query_definition, design=self.design, suppress_warnings=self.suppress_warnings
            )

        # If query type does not allow semicolons
        if self.query_type in Definitions.no_semicolon_warehouses:
            semicolon = False
        query = query_generator.get_query(semicolon=semicolon)

        return query

    def get_used_views(self):
        unique_view_names = {f.view.name for f in self.field_lookup.values()}
        return [self.project.get_view(name) for name in unique_view_names]

    def parse_where(self, where: list):
        if where is None or where == [] or self._is_literal(where):
            return where
        where_with_query = []
        for w in where:
            if "funnel" in w:
                non_funnel_where = [f for f in where if "funnel" not in f and "group_by" not in f]
                funnel_query = {
                    "metrics": self.metrics,
                    "dimensions": [],
                    "funnel": w["funnel"],
                    "where": non_funnel_where,
                    "having": self.having,
                }
                w["query_class"] = FunnelQuery(
                    funnel_query, design=self.design, suppress_warnings=self.suppress_warnings
                )

            if "conditions" in w:
                conditions = w["conditions"]
            else:
                conditions = []
            if conditions:
                field_types = set(
                    [
                        self.field_lookup[f["field"]].field_type
                        for f in self.flatten_filters(conditions)
                        if "group_by" not in f
                    ]
                )
                if "measure" in field_types and (
                    "dimension" in field_types or "dimension_group" in field_types
                ):
                    raise QueryError(
                        "Cannot mix dimensions and measures in a compound filter with a logical_operator"
                    )
            where_with_query.append(w)
        return where_with_query

    def parse_having(self, having: list):
        if having is None or having == [] or self._is_literal(having):
            return having
        validated_having = []
        for h in having:
            if "conditions" in h:
                conditions = h["conditions"]
            else:
                conditions = []
            if conditions:
                field_types = set(
                    [self.field_lookup[f["field"]].field_type for f in self.flatten_filters(conditions)]
                )
                if "measure" in field_types and (
                    "dimension" in field_types or "dimension_group" in field_types
                ):
                    raise QueryError(
                        "Cannot mix dimensions and measures in a compound filter with a logical_operator"
                    )
            validated_having.append(h)
        return validated_having

    def parse_input(self):
        all_field_names = self.metrics + self.dimensions
        if len(set(all_field_names)) != len(all_field_names):
            raise QueryError("Ambiguous field names in the metrics and dimensions")

        for name in self.metrics:
            field = self.get_field_with_error_handling(name, "Metric")
            if field.is_cumulative():
                self.has_cumulative_metric = True
            self.field_lookup[name] = field

        metric_view = None if len(self.metrics) == 0 else self.field_lookup[self.metrics[0]].view.name

        for name in self.dimensions:
            field = self.get_field_with_error_handling(name, "Dimension")
            # We will not use a group by if the primary key of the main resulting table is included
            if field.primary_key and field.view.name == metric_view and not self.force_group_by:
                self.no_group_by = True
            self.field_lookup[name] = field

        for name in self._where_field_names:
            self.field_lookup[name] = self.get_field_with_error_handling(name, "Where clause field")

        for name in self._having_field_names:
            self.field_lookup[name] = self.get_field_with_error_handling(name, "Having clause field")

        for name in self._order_by_field_names:
            self.field_lookup[name] = self.get_field_with_error_handling(name, "Order by field")

    def get_field_with_error_handling(self, field_name: str, error_prefix: str):
        field = self.project.get_field(field_name, model=self.model)
        if field is None:
            raise QueryError(f"{error_prefix} {field_name} not found")
        return field

    def parse_field_names(self, where, having, order_by):
        self.where = self._check_for_dict(where)
        if self._is_literal(self.where):
            self._where_field_names = MetricsLayerQuery.parse_identifiers_from_clause(self.where)
        else:
            self._where_field_names = self.parse_identifiers_from_dicts(self.where)

        self.having = self._check_for_dict(having)
        if self._is_literal(self.having):
            self._having_field_names = MetricsLayerQuery.parse_identifiers_from_clause(self.having)
        else:
            self._having_field_names = self.parse_identifiers_from_dicts(self.having)

        self.order_by = self._check_for_dict(order_by)
        if self._is_literal(self.order_by):
            self._order_by_field_names = MetricsLayerQuery.parse_identifiers_from_clause(self.order_by)
        else:
            self._order_by_field_names = self.parse_identifiers_from_dicts(self.order_by)
        return self._where_field_names, self._having_field_names, self._order_by_field_names

    def parse_funnel(self, funnel: dict):
        if funnel != {}:
            if all(k in funnel for k in ["steps", "within"]):
                is_funnel_query = True
            else:
                raise QueryError("Funnel query must have 'steps' and 'within' keys")
        else:
            is_funnel_query = False
        return funnel, is_funnel_query

    @staticmethod
    def _is_literal(clause):
        return isinstance(clause, str) or clause is None

    def parse_identifiers_from_dicts(self, conditions: list):
        flattened_conditions = SingleSQLQueryResolver.flatten_filters(conditions)
        try:
            field_names = []
            for cond in flattened_conditions:
                if "group_by" in cond:
                    field_names.append(cond["group_by"])
                else:
                    field_names.append(cond["field"])
                if "value" in cond and isinstance(cond["value"], str):
                    mapped_field = self.project.get_mapped_field(cond["value"], model=self.model)
                    if mapped_field:
                        field_names.append(cond["value"])
            return field_names
        except KeyError:
            for cond in conditions:
                if "field" not in cond:
                    break
            raise QueryError(f"Identifier was missing required 'field' key: {cond}")

    @staticmethod
    def flatten_filters(filters: list, return_nesting_depth: bool = False):
        return flatten_filters(filters, return_nesting_depth=return_nesting_depth)

    @staticmethod
    def _check_for_dict(conditions: list):
        if isinstance(conditions, dict):
            return [conditions]
        return conditions
