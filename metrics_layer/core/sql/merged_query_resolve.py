from metrics_layer.core.exceptions import QueryError
from metrics_layer.core.parse.config import MetricsLayerConfiguration
from collections import defaultdict
from copy import deepcopy

from metrics_layer.core.sql.query_merged_results import MetricsLayerMergedResultsQuery
from metrics_layer.core.sql.single_query_resolve import SingleSQLQueryResolver


class MergedSQLQueryResolver(SingleSQLQueryResolver):
    def __init__(
        self,
        metrics: list,
        dimensions: list = [],
        funnel: dict = {},
        where: str = None,  # Either a list of json or a string
        having: str = None,  # Either a list of json or a string
        order_by: str = None,  # Either a list of json or a string
        model=None,
        config: MetricsLayerConfiguration = None,
        **kwargs,
    ):
        if funnel != {}:
            raise QueryError("Funnel queries are not supported in merged results queries")

        self.field_lookup = {}
        self.no_group_by = False
        self.has_cumulative_metric = False
        self.verbose = kwargs.get("verbose", False)
        self.select_raw_sql = kwargs.get("select_raw_sql", [])
        self.suppress_warnings = kwargs.get("suppress_warnings", False)
        self.limit = kwargs.get("limit")
        self.return_pypika_query = kwargs.get("return_pypika_query")
        self.force_group_by = kwargs.get("force_group_by", False)
        self.kwargs = kwargs
        self.config = config
        self.project = self.config.project
        self.metrics = metrics
        self.dimensions = dimensions
        self.parse_field_names(where, having, order_by)
        self.model = model

    def get_query(self, semicolon: bool = True):
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
                    "values for metrics in the same subquery for a merged result"
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
            field_key = f"{field.view.name}.{field.name}"
            join_group_hash = self.project.join_graph.join_graph_hash(field.view.name)
            if field_key in canon_dates:
                join_hash = f'{field_key.replace(".", "_")}__{join_group_hash}'
                self.query_dimensions[join_hash].append(field)

                dimension_group = field.dimension_group
                for mapping_info in dimension_mapping[field_key]:
                    if mapping_info["from_join_hash"] in self.query_metrics:
                        key = f"{mapping_info['field']}_{dimension_group}"
                        ref_field = self.project.get_field(key)
                        self.query_dimensions[mapping_info["from_join_hash"]].append(ref_field)
            else:
                not_in_metrics = True
                for join_hash in self.query_metrics.keys():
                    # If the dimension is available in the join subgraph as the metric, attach it
                    if any(jg in join_hash for jg in field.join_graphs()):
                        self.query_dimensions[join_hash].append(field)
                        not_in_metrics = False
                    else:
                        if field_key not in dimension_mapping:
                            raise QueryError(
                                f"Could not find mapping from field {field_key} to other views. "
                                "Please add a mapping to your model definition to allow the mapping "
                                "if you'd like to use this field in a merged result query."
                            )
                        for mapping_info in dimension_mapping[field_key]:
                            if mapping_info["from_join_hash"] in self.query_metrics:

                                ref_field = self.project.get_field(mapping_info["field"])
                                self.query_dimensions[mapping_info["from_join_hash"]].append(ref_field)
                if not_in_metrics:
                    canon_date = field.canon_date.replace(".", "_")
                    key = f"{canon_date}__{join_group_hash}"
                    self.query_metrics[key] = []
                    self.query_dimensions[key].append(field)

        self.query_dimensions = self.deduplicate_fields(self.query_dimensions)

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
                        if dimension_group:
                            key = f"{mapping_info['field']}_{dimension_group}"
                        else:
                            key = mapping_info["field"]
                        ref_field = self.project.get_field(key)
                        mapped_where = deepcopy(where)
                        mapped_where["field"] = ref_field.id()
                        self.query_where[join_hash].append(mapped_where)

    @staticmethod
    def deduplicate_fields(field_dict: dict):
        # Get rid of duplicates while keeping order to make joining work properly
        return {k: sorted(list(set(v)), key=lambda x: v.index(x)) for k, v in field_dict.items()}
