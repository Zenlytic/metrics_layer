from collections import defaultdict
from copy import deepcopy

from metrics_layer.core.exceptions import QueryError
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
        funnel: dict = {},  # A dict with steps (list) and within (string)
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
        self.funnel = funnel
        self.where = where
        self.having = having
        self.order_by = order_by
        self.kwargs = kwargs
        self.connection = None

        model_name = self.kwargs.get("model_name")
        models = self.project.models()

        # If you specify the model that's top priority
        if model_name:
            self.model = self.project.get_model(model_name)
        # Otherwise, if there's only one option, we use that
        elif len(models) == 1:
            self.model = models[0]
        # Finally, check views for models
        else:
            self.model = self._derive_model()

    def get_query(self, semicolon: bool = True):
        if self.merged_result:
            return self._get_merged_result_query(semicolon=semicolon)
        return self._get_single_query(semicolon=semicolon)

    def _get_single_query(self, semicolon: bool):
        resolver = SingleSQLQueryResolver(
            metrics=self.metrics,
            dimensions=self.dimensions,
            funnel=self.funnel,
            where=self.where,
            having=self.having,
            order_by=self.order_by,
            model=self.model,
            config=self.config,
            **self.kwargs,
        )
        query = resolver.get_query(semicolon)
        self.connection = resolver.connection
        return query

    def _get_merged_result_query(self, semicolon: bool):
        self.parse_field_names(self.where, self.having, self.order_by)
        self.derive_sub_queries()

        queries_to_join = {}
        for join_hash in self.query_metrics.keys():
            metrics = [f.id() for f in self.query_metrics[join_hash]]
            dimensions = [f.id() for f in self.query_dimensions.get(join_hash, [])]

            # Overwrite the limit arg because these are subqueries
            kws = {**self.kwargs, "limit": None, "return_pypika_query": True}
            resolver = SingleSQLQueryResolver(
                metrics=metrics,
                dimensions=dimensions,
                where=self.query_where[join_hash],
                having=[],
                order_by=[],
                model=self.model,
                config=self.config,
                **kws,
            )
            query = resolver.get_query(semicolon=False)
            queries_to_join[join_hash] = query

        query_config = {
            "merged_metrics": self.merged_metrics,
            "query_metrics": self.query_metrics,
            "query_dimensions": self.query_dimensions,
            "having": self.having,
            "queries_to_join": queries_to_join,
            "join_hashes": list(sorted(self.query_metrics.keys())),
            "query_type": resolver.query_type,
            "limit": self.limit,
            "project": self.project,
        }
        merged_result_query = MetricsLayerMergedResultsQuery(query_config)
        query = merged_result_query.get_query(semicolon=semicolon)

        self.connection = resolver.connection
        return query

    def derive_sub_queries(self):
        self.query_metrics = defaultdict(list)
        self.merged_metrics = []
        self.secondary_metrics = []

        for metric in self.metrics:
            field = self.project.get_field(metric)
            if field.is_merged_result:
                self.merged_metrics.append(field)
            else:
                self.secondary_metrics.append(field)

        for merged_metric in self.merged_metrics:
            for ref_field in merged_metric.referenced_fields(merged_metric.sql):
                if isinstance(ref_field, str):
                    raise QueryError(f"Unable to find the field {ref_field} in the project")

                join_group_hash = self.project.join_graph.join_graph_hash(ref_field.view.name)
                canon_date = ref_field.canon_date.replace(".", "_")
                key = f"{canon_date}__{join_group_hash}"
                self.query_metrics[key].append(ref_field)

        for field in self.secondary_metrics:
            join_group_hash = self.project.join_graph.join_graph_hash(field.view.name)
            canon_date = field.canon_date.replace(".", "_")
            key = f"{canon_date}__{join_group_hash}"
            if key in self.query_metrics:
                already_in_query = any(field.id() in f.id() for f in self.query_metrics[key])
                if not already_in_query:
                    self.query_metrics[key].append(field)
            else:
                self.query_metrics[key].append(field)

        canon_dates = []
        dimension_mapping = defaultdict(list)
        for join_hash, field_set in self.query_metrics.items():
            if len({f.canon_date for f in field_set}) > 1:
                raise NotImplementedError(
                    "Zenlytic does not currently support different canon_date "
                    "values for metrics in the same subquery"
                )
            canon_date = field_set[0].canon_date
            canon_dates.append(canon_date)
            for other_explore_name, other_field_set in self.query_metrics.items():
                if other_explore_name != join_hash:
                    other_canon_date = other_field_set[0].canon_date
                    canon_date_data = {"field": other_canon_date, "from_join_hash": other_explore_name}
                    dimension_mapping[canon_date].append(canon_date_data)

        mappings = self.model.get_mappings()
        for key, map_to in mappings.items():
            for other_join_hash, other_field_set in self.query_metrics.items():
                if map_to["to_join_hash"] in other_join_hash:
                    map_to["from_join_hash"] = other_join_hash
                dimension_mapping[key].append(deepcopy(map_to))

        self.query_dimensions = defaultdict(list)
        for dimension in self.dimensions:
            field = self.project.get_field(dimension)
            join_group_hash = self.project.join_graph.join_graph_hash(field.view.name)
            field_key = f"{field.view.name}.{field.name}"
            if field_key in canon_dates:
                join_hash = f'{field_key.replace(".", "_")}__{join_group_hash}'
                self.query_dimensions[join_hash].append(field)

                dimension_group = field.dimension_group
                for mapping_info in dimension_mapping[field_key]:
                    key = f"{mapping_info['field']}_{dimension_group}"
                    ref_field = self.project.get_field(key)
                    self.query_dimensions[mapping_info["from_join_hash"]].append(ref_field)
            else:
                for join_hash in self.query_metrics.keys():
                    if join_group_hash in join_hash:
                        self.query_dimensions[join_hash].append(field)
                    else:
                        if field_key not in dimension_mapping:
                            raise QueryError(
                                f"Could not find mapping from field {field_key} to other views. "
                                "Please add a mapping to your view definition to allow this."
                            )
                        for mapping_info in dimension_mapping[field_key]:
                            ref_field = self.project.get_field(mapping_info["field"])
                            self.query_dimensions[mapping_info["from_join_hash"]].append(ref_field)

        # Get rid of duplicates while keeping order to make joining work properly
        self.query_dimensions = {
            k: sorted(list(set(v)), key=lambda x: v.index(x)) for k, v in self.query_dimensions.items()
        }

        self.query_where = defaultdict(list)
        for where in self.where:
            field = self.project.get_field(where["field"])
            dimension_group = field.dimension_group
            join_group_hash = self.project.join_graph.join_graph_hash(field.view.name)
            for join_hash in self.query_metrics.keys():
                if join_group_hash in join_hash:
                    self.query_where[join_hash].append(where)
                else:
                    key = f"{field.view.name}.{field.name}"
                    for mapping_info in dimension_mapping[key]:
                        key = f"{mapping_info['field']}_{dimension_group}"
                        ref_field = self.project.get_field(key)
                        mapped_where = deepcopy(where)
                        mapped_where["field"] = ref_field.id()
                        join_group_hash = self.project.join_graph.join_graph_hash(ref_field.view.name)
                        self.query_where[join_hash].append(mapped_where)

    def _derive_model(self):
        all_fields = self.metrics + self.dimensions
        all_model_names = {f.view.model_name for f in all_fields}

        if len(all_model_names) == 0:
            raise QueryError(
                "No models found in this data model. Please specify a model "
                "to connect a data warehouse to your data model."
            )
        elif len(all_model_names) == 1:
            return self.project.get_model(list(all_model_names)[0])
        else:
            raise QueryError(
                "More than one model found in this data model. Please specify a model "
                "to use by either passing the name of the model using 'model_name' parameter or by  "
                "setting the `model_name` property on the view."
            )
