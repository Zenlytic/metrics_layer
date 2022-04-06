from collections import defaultdict
from copy import deepcopy

from metrics_layer.core.parse.config import MetricsLayerConfiguration
from metrics_layer.core.sql.query_merged_results import MetricsLayerMergedResultsQuery
from metrics_layer.core.sql.single_query_resolve import SingleSQLQueryResolver


class SQLQueryResolver(SingleSQLQueryResolver):
    """
    Method of resolving the explore name:
        if there is not explore passed (using the format explore_name.field_name), we'll search for
        just the field name and iff that field is used in only one explore, set that as the active explore.
            - Any fields specified that are not in that explore will raise an error

        if it's passed explicitly, use the first metric's explore, and raise an error if anything conflicts
        with that explore
    """

    def __init__(
        self,
        metrics: list,
        dimensions: list = [],
        where: str = None,  # Either a list of json or a string
        having: str = None,  # Either a list of json or a string
        order_by: str = None,  # Either a list of json or a string
        config: MetricsLayerConfiguration = None,
        **kwargs,
    ):
        self.field_lookup = {}
        self.no_group_by = False
        self.verbose = kwargs.get("verbose", False)
        self.merged_result = kwargs.get("merged_result", False)
        self.select_raw_sql = kwargs.get("select_raw_sql", [])
        self.explore_name = kwargs.get("explore_name")
        self.suppress_warnings = kwargs.get("suppress_warnings", False)
        self.limit = kwargs.get("limit")
        self.config = config
        self.project = self.config.project
        self.metrics = metrics
        self.dimensions = dimensions
        self.where = where
        self.having = having
        self.order_by = order_by
        self.kwargs = kwargs
        self.connection = None

    def get_query(self, semicolon: bool = True):
        if self.merged_result:
            return self._get_merged_result_query(semicolon=semicolon)
        return self._get_single_query(semicolon=semicolon)

    def _get_single_query(self, semicolon: bool):
        resolver = SingleSQLQueryResolver(
            metrics=self.metrics,
            dimensions=self.dimensions,
            where=self.where,
            having=self.having,
            order_by=self.order_by,
            config=self.config,
            **self.kwargs,
        )
        query = resolver.get_query(semicolon)
        self.connection = resolver.connection
        return query

    def _get_merged_result_query(self, semicolon: bool):
        self.kwargs.pop("explore_name", None)

        self.parse_field_names(self.where, self.having, self.order_by)
        self.derive_sub_queries()

        explore_queries = {}
        for explore_name in self.explore_metrics.keys():
            metrics = [f.id(view_only=True) for f in self.explore_metrics[explore_name]]
            dimensions = [f.id(view_only=True) for f in self.explore_dimensions[explore_name]]

            # Overwrite the limit arg because these are subqueries
            kws = {**self.kwargs, "limit": None, "return_pypika_query": True}
            resolver = SingleSQLQueryResolver(
                metrics=metrics,
                dimensions=dimensions,
                where=self.explore_where[explore_name],
                having=[],
                order_by=[],
                config=self.config,
                explore_name=explore_name,
                **kws,
            )
            query = resolver.get_query(semicolon=False)
            explore_queries[explore_name] = query

        query_config = {
            "merged_metrics": self.merged_metrics,
            "explore_metrics": self.explore_metrics,
            "explore_dimensions": self.explore_dimensions,
            "having": self.having,
            "explore_queries": explore_queries,
            "explore_names": list(sorted(self.explore_metrics.keys())),
            "query_type": resolver.query_type,
            "limit": self.limit,
            "project": self.project,
        }
        merged_result_query = MetricsLayerMergedResultsQuery(query_config)
        query = merged_result_query.get_query(semicolon=semicolon)

        self.connection = resolver.connection
        return query

    def derive_sub_queries(self):
        # The different explores used are determined by the metrics referenced
        self.explore_metrics = defaultdict(list)
        self.merged_metrics = []
        for metric in self.metrics:
            explore_name = self.project.get_explore_from_field(metric)
            field = self.project.get_field(metric, explore_name=explore_name)
            if field.is_merged_result:
                self.merged_metrics.append(field)
            else:
                if field.canon_date is None:
                    raise ValueError(
                        "You must specify the canon_date property if you want to use a merged result query"
                    )
                self.explore_metrics[explore_name].append(field)

        for merged_metric in self.merged_metrics:
            for ref_field in merged_metric.referenced_fields(merged_metric.sql, ignore_explore=True):
                if isinstance(ref_field, str):
                    raise ValueError(f"Unable to find the field {ref_field} in the project")
                if ref_field.view.explore is None:
                    explore_name = merged_metric.view.explore.name
                else:
                    explore_name = ref_field.view.explore.name
                self.explore_metrics[explore_name].append(ref_field)

        for explore_name in self.explore_metrics.keys():
            self.explore_metrics[explore_name] = list(set(self.explore_metrics[explore_name]))

        dimension_mapping = defaultdict(list)
        for explore_name, field_set in self.explore_metrics.items():
            if len({f.canon_date for f in field_set}) > 1:
                raise NotImplementedError(
                    "Zenlytic does not currently support different canon_date "
                    "values for metrics in the same query in the same explore"
                )
            canon_date = field_set[0].canon_date
            for other_explore_name, other_field_set in self.explore_metrics.items():
                if other_explore_name != explore_name:
                    other_canon_date = other_field_set[0].canon_date
                    other_view_name = other_field_set[0].view.name
                    canon_date_data = {
                        "field": f"{other_view_name}.{other_canon_date}",
                        "explore_name": other_explore_name,
                    }
                    dimension_mapping[canon_date].append(canon_date_data)

        self.explore_dimensions = defaultdict(list)
        for dimension in self.dimensions:
            explore_name = self.project.get_explore_from_field(dimension)
            if explore_name not in self.explore_metrics:
                raise ValueError(
                    f"Could not find a metric in {self.metrics} that references the explore {explore_name}"
                )
            field = self.project.get_field(dimension, explore_name=explore_name)
            dimension_group = field.dimension_group
            self.explore_dimensions[explore_name].append(field)
            for mapping_info in dimension_mapping[field.name]:
                key = f"{mapping_info['field']}_{dimension_group}"
                field = self.project.get_field(key, explore_name=mapping_info["explore_name"])
                self.explore_dimensions[mapping_info["explore_name"]].append(field)

        self.explore_where = defaultdict(list)
        for where in self.where:
            explore_name = self.project.get_explore_from_field(where["field"])
            if explore_name not in self.explore_metrics:
                raise ValueError(
                    f"Could not find a metric in {self.metrics} that references the explore {explore_name}"
                )
            field = self.project.get_field(where["field"], explore_name=explore_name)
            dimension_group = field.dimension_group
            self.explore_where[explore_name].append(where)
            for mapping_info in dimension_mapping[field.name]:
                key = f"{mapping_info['field']}_{dimension_group}"
                field = self.project.get_field(key, explore_name=mapping_info["explore_name"])
                mapped_where = deepcopy(where)
                mapped_where["field"] = field.id(view_only=True)
                self.explore_where[mapping_info["explore_name"]].append(mapped_where)
