import json
import hashlib
from metrics_layer.core.exceptions import QueryError
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
        project=None,
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
        self.project = project
        self.metrics = metrics
        self.dimensions = dimensions
        self.parse_field_names(where, having, order_by)
        self.model = model

    def get_query(self, semicolon: bool = True):
        self.parse_field_names(self.where, self.having, self.order_by)
        self.derive_sub_queries()

        queries_to_join = {}
        join_hashes = list(self.query_metrics.keys())
        for k in self.query_dimensions.keys():
            if k not in join_hashes:
                join_hashes.append(k)
        for join_hash in join_hashes:
            metrics = [f.id() for f in self.query_metrics.get(join_hash, [])]
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
                project=self.project,
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
            "join_hashes": list(sorted(join_hashes)),
            "query_type": resolver.query_type,
            "limit": self.limit,
            "project": self.project,
        }
        merged_result_query = MetricsLayerMergedResultsQuery(query_config)
        query = merged_result_query.get_query(semicolon=semicolon)

        return query

    def derive_sub_queries(self):
        self.query_metrics = defaultdict(list)
        self.merged_metrics = []
        self.secondary_metrics = []
        self.dimension_fields = []

        for metric in self.metrics:
            field = self.project.get_field(metric)
            if field.is_merged_result:
                self.merged_metrics.append(field)
            else:
                self.secondary_metrics.append(field)

        for dimension in self.dimensions:
            self.dimension_fields.append(self.project.get_field(dimension))

        for merged_metric in self.merged_metrics:
            for ref_field in merged_metric.referenced_fields(merged_metric.sql):
                if isinstance(ref_field, str):
                    raise QueryError(f"Unable to find the field {ref_field} in the project")
                if ref_field.canon_date is None:
                    raise QueryError(
                        f"Could not find a date field associated with metric {ref_field.name} "
                        "in the project. \n\nMake sure you have this defined either in the view "
                        "with the property 'default_date' or on the metric under 'canon_date'"
                    )
                join_group_hash = self.project.join_graph.join_graph_hash(ref_field.view.name)
                key = self._cte_name_from_parts(ref_field.canon_date, join_group_hash)
                self.query_metrics[key].append(ref_field)

        for field in self.secondary_metrics:
            join_group_hash = self.project.join_graph.join_graph_hash(field.view.name)
            if field.canon_date is None:
                raise QueryError(
                    f"Could not find a date field associated with metric {ref_field.name} "
                    "in the project. \n\nMake sure you have this defined either in the view "
                    "with the property 'default_date' or on the metric under 'canon_date'"
                )
            key = self._cte_name_from_parts(field.canon_date, join_group_hash)
            if key in self.query_metrics:
                already_in_query = any(field.id() in f.id() for f in self.query_metrics[key])
                if not already_in_query:
                    self.query_metrics[key].append(field)
            else:
                self.query_metrics[key].append(field)

        self.query_metrics = self.deduplicate_fields(self.query_metrics)

        dimension_mapping, canon_dates, used_join_hashes = self._canon_date_mapping()

        mappings = self.model.get_mappings(dimensions_only=True)
        for key, map_to in mappings.items():
            for other_join_hash in used_join_hashes:
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
                    key = f"{mapping_info['field']}_{dimension_group}"
                    ref_field = self.project.get_field(key)
                    self.query_dimensions[mapping_info["from_join_hash"]].append(ref_field)
            else:
                not_in_metrics = True
                for join_hash in used_join_hashes:
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
                            ref_field = self.project.get_field(mapping_info["field"])
                            if mapping_info["from_join_hash"] in self.query_metrics:
                                self.query_dimensions[mapping_info["from_join_hash"]].append(ref_field)
                            else:
                                canon_date = ref_field.canon_date.replace(".", "_")
                                key = f"{canon_date}__{mapping_info['to_join_hash']}"
                                self.query_dimensions[key].append(ref_field)

                if not_in_metrics:
                    field_key = f"{field.view.name}.{field.name}"
                    canon_date = field.canon_date.replace(".", "_")
                    key = f"{canon_date}__{join_group_hash}"
                    self.query_metrics[key] = []
                    self.query_dimensions[key].append(field)
                    if field_key in dimension_mapping:
                        for mapping_info in dimension_mapping[field_key]:
                            ref_field = self.project.get_field(mapping_info["field"])
                            canon_date = ref_field.canon_date.replace(".", "_")
                            mapped_key = f"{canon_date}__{mapping_info['to_join_hash']}"
                            self.query_dimensions[mapped_key].append(ref_field)

        self.query_dimensions = self.deduplicate_fields(self.query_dimensions)

        keys = list(self.query_metrics.keys()) + list(self.query_dimensions.keys())
        unique_keys = sorted(list(set(keys)), key=lambda x: keys.index(x))
        self.query_where = defaultdict(list)
        for where in self.where:
            field = self.project.get_field(where["field"])
            dimension_group = field.dimension_group
            join_group_hash = self.project.join_graph.join_graph_hash(field.view.name)
            added_filter = False
            for join_hash in unique_keys:
                if join_group_hash in join_hash:
                    self.query_where[join_hash].append(where)
                    added_filter = True
                else:
                    key = f"{field.view.name}.{field.name}"
                    for mapping_info in dimension_mapping[key]:
                        if mapping_info["from_join_hash"] == join_hash:
                            if dimension_group:
                                key = f"{mapping_info['field']}_{dimension_group}"
                            else:
                                key = mapping_info["field"]
                            ref_field = self.project.get_field(key)
                            mapped_where = deepcopy(where)
                            mapped_where["field"] = ref_field.id()
                            self.query_where[join_hash].append(mapped_where)
            if not added_filter:
                # This handles the case where the where field is joined in and not in a mapping
                for join_hash in self.query_metrics.keys():
                    self.query_where[join_hash].append(where)

        clean_wheres = defaultdict(list)
        for k, v in self.query_where.items():
            hashes, lookup = [], {}
            for item in v:
                h = self.hash_dict(item)
                lookup[h] = item
                hashes.append(h)

            sorted_hashes = sorted(list(set(hashes)), key=lambda x: hashes.index(x))
            clean_wheres[k] = [lookup[h] for h in sorted_hashes]
        self.query_where = clean_wheres

    def _canon_date_mapping(self):
        canon_dates, join_hashes = [], []
        dimension_mapping = defaultdict(list)
        for merged_metric in self.merged_metrics:
            for ref_field in merged_metric.referenced_fields(merged_metric.sql):
                canon_dates.append(ref_field.canon_date)

        joinable_sets = []
        for field in self.secondary_metrics + self.dimension_fields:
            joinable_graphs = [j for j in field.join_graphs() if "merged_result" not in j]
            joinable = all(any(j in join_set for j in joinable_graphs) for join_set in joinable_sets)

            cannot_join = not joinable or len(joinable_sets) == 0
            measure_date_missing = field.canon_date not in canon_dates and field.field_type == "measure"
            if field.canon_date and (cannot_join or measure_date_missing):
                canon_dates.append(field.canon_date)
                joinable_sets.append(joinable_graphs)

        canon_dates = list(sorted(list(set(canon_dates))))
        for to_canon_date_name in canon_dates:
            for from_canon_date_name in canon_dates:
                if to_canon_date_name != from_canon_date_name:
                    from_canon_date = self.project.get_field_by_name(from_canon_date_name)
                    join_group_hash = self.project.join_graph.join_graph_hash(from_canon_date.view.name)
                    key = self._cte_name_from_parts(from_canon_date_name, join_group_hash)
                    canon_date_data = {"field": from_canon_date_name, "from_join_hash": key}
                    dimension_mapping[to_canon_date_name].append(canon_date_data)
                    join_hashes.append(key)

        sorted_joins = sorted(
            list(set(join_hashes)),
            key=lambda x: str(list(self.query_metrics.keys()).index(x)) if x in self.query_metrics else x,
        )
        return dimension_mapping, canon_dates, sorted_joins

    @staticmethod
    def _cte_name_from_parts(field_id: str, join_group_hash: str):
        canon_date = field_id.replace(".", "_")
        return f"{canon_date}__{join_group_hash}"

    @staticmethod
    def deduplicate_fields(field_dict: dict):
        # Get rid of duplicates while keeping order to make joining work properly
        return {k: sorted(list(set(v)), key=lambda x: v.index(x)) for k, v in field_dict.items()}

    @staticmethod
    def hash_dict(input_dict: dict):
        as_strings = {k: str(v) for k, v in input_dict.items()}
        return hashlib.sha256(json.dumps(as_strings, sort_keys=True).encode("utf-8")).hexdigest()
