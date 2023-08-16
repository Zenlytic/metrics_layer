import json
import hashlib
from metrics_layer.core.exceptions import QueryError
from collections import defaultdict
from copy import deepcopy

from metrics_layer.core.model.definitions import Definitions
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

        join_hash_readability_lookup = {
            j: f"{j.split('__')[0]}__cte_subquery_{i}" for i, j in enumerate(sorted(join_hashes))
        }

        readable_join_hashes = []
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
            readable_hash = join_hash_readability_lookup[join_hash]
            readable_join_hashes.append(readable_hash)
            queries_to_join[readable_hash] = query

        readable_metrics = {join_hash_readability_lookup[k]: m for k, m in self.query_metrics.items()}
        readable_dimensions = {join_hash_readability_lookup[k]: d for k, d in self.query_dimensions.items()}
        readable_mapping_lookup = {
            k: [
                {"cte": join_hash_readability_lookup[v["from_join_hash"]], "field": v["field"]} for v in items
            ]
            for k, items in self.mapping_lookup.items()
        }
        query_config = {
            "merged_metrics": self.merged_metrics,
            "query_metrics": readable_metrics,
            "query_dimensions": readable_dimensions,
            "having": self.having,
            "queries_to_join": queries_to_join,
            "join_hashes": list(sorted(readable_join_hashes)),
            "mapping_lookup": readable_mapping_lookup,
            "query_type": resolver.query_type,
            "limit": self.limit,
            "project": self.project,
        }
        # Druid does not allow semicolons
        if resolver.query_type == Definitions.druid:
            semicolon = False

        merged_result_query = MetricsLayerMergedResultsQuery(query_config)
        query = merged_result_query.get_query(semicolon=semicolon)

        return query

    def derive_sub_queries(self):
        self.query_metrics = defaultdict(list)
        self.merged_metrics = []
        self.secondary_metrics = []
        self.dimension_fields = []
        self.mapping_lookup = {}

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
                join_group_hash = self._join_hash_key(ref_field)
                key = self._cte_name_from_parts(ref_field.canon_date, join_group_hash)
                self.query_metrics[key].append(ref_field)

        for field in self.secondary_metrics:
            join_group_hash = self._join_hash_key(field)
            if field.canon_date is None:
                raise QueryError(
                    f"Could not find a date field associated with metric {field.name} "
                    "in the project. \n\nMake sure you have this defined either in the view "
                    "with the property 'default_date' or on the metric under 'canon_date'"
                )
            key = self._cte_name_from_parts(field.canon_date, join_group_hash)
            if key in self.query_metrics:
                already_in_query = any(field.id() == f.id() for f in self.query_metrics[key])
                if not already_in_query:
                    self.query_metrics[key].append(field)
            else:
                self.query_metrics[key].append(field)

        self.query_metrics = self.deduplicate_fields(self.query_metrics)

        dimension_mapping, canon_dates, used_join_hashes = self._canon_date_mapping()
        self.mapping_lookup = deepcopy(dimension_mapping)

        mappings = self.model.get_mappings(dimensions_only=True)
        for key, map_to in mappings.items():
            for other_join_hash in used_join_hashes:
                if self._join_hash_contains_join_graph(other_join_hash, [map_to["to_join_hash"]]):
                    self.mapping_lookup[key].append(
                        {"field": map_to["field"], "from_join_hash": other_join_hash}
                    )
                    map_to["from_join_hash"] = other_join_hash
                dimension_mapping[key].append(deepcopy(map_to))

        self.query_dimensions = defaultdict(list)
        for dimension in self.dimensions:
            field = self.project.get_field(dimension)
            field_key = f"{field.view.name}.{field.name}"
            join_group_hash = self._join_hash_key(field)
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
                    # If the dimension is available in the same join subgraph as the metric, attach it
                    if self._join_hash_contains_join_graph(join_hash, field.join_graphs()):
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
                                existing_join_hashes = (
                                    join_hash
                                    for join_hash in used_join_hashes
                                    if mapping_info["to_join_hash"] in join_hash
                                )
                                join_hash = next(existing_join_hashes, None)
                                default_join_hash = f"{canon_date}__{mapping_info['to_join_hash']}"
                                key = join_hash if join_hash else default_join_hash
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
            metric_canon_dates = {f.canon_date for v in self.query_metrics.values() for f in v}
            field = self.project.get_field(where["field"])
            is_canon_date = any(f"{field.view.name}.{field.name}" == d for d in metric_canon_dates)
            dimension_group = field.dimension_group
            join_group_hash = self._join_hash_key(field)
            added_filter = {join_hash: False for join_hash in unique_keys}
            for join_hash in unique_keys:
                join_hash_with_canon_date = f"{field.view.name}_{field.name}__{join_group_hash}"
                joinable_graphs = join_hash.split("__")[-1]
                # The field is joinable if the subquery is the same as one in the main join hash's subquery
                joinable_subqueries = [
                    f"subquery{g}".strip("_") for g in joinable_graphs.split("subquery") if g != ""
                ]
                joinable_not_canon_date = not is_canon_date and join_group_hash in joinable_subqueries
                is_canon_date_same = is_canon_date and join_hash_with_canon_date in join_hash
                if joinable_not_canon_date or is_canon_date_same:
                    self.query_where[join_hash].append(where)
                    added_filter[join_hash] = True
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
                            added_filter[join_hash] = True
            # This handles the case where the where field is joined in and not in a mapping
            for join_hash in self.query_metrics.keys():
                if not added_filter[join_hash]:
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
            all_joinable = all(any(j in join_set for j in joinable_graphs) for join_set in joinable_sets)
            any_joinable = any(any(j in join_set for j in joinable_graphs) for join_set in joinable_sets)

            is_not_measure = field.field_type != "measure"
            is_measure = field.field_type == "measure"
            cannot_join_to_all = not all_joinable or len(joinable_sets) == 0
            measure_date_missing = field.canon_date not in canon_dates and is_measure
            if len(self.secondary_metrics + self.merged_metrics) > 0:
                measure_cannot_join_to_all = cannot_join_to_all and is_measure
                dim_cannot_join_to_any = is_not_measure and not any_joinable
            else:
                measure_cannot_join_to_all = cannot_join_to_all
                dim_cannot_join_to_any = is_not_measure and not any_joinable

            if field.canon_date and (
                measure_cannot_join_to_all or dim_cannot_join_to_any or measure_date_missing
            ):
                canon_dates.append(field.canon_date)
                joinable_sets.append(joinable_graphs)

        canon_dates = list(sorted(list(set(canon_dates))))
        for to_canon_date_name in canon_dates:
            for from_canon_date_name in canon_dates:
                if to_canon_date_name != from_canon_date_name:
                    from_canon_date = self.project.get_field_by_name(from_canon_date_name)
                    join_group_hash = self._join_hash_key(from_canon_date)
                    key = self._cte_name_from_parts(from_canon_date_name, join_group_hash)
                    canon_date_data = {"field": from_canon_date_name, "from_join_hash": key}
                    dimension_mapping[to_canon_date_name].append(canon_date_data)
                    join_hashes.append(key)

        sorted_joins = sorted(
            list(set(join_hashes)),
            key=lambda x: str(list(self.query_metrics.keys()).index(x)) if x in self.query_metrics else x,
        )
        return dimension_mapping, canon_dates, sorted_joins

    def _join_hash_key(self, field):
        # This makes the join hash out of all the joinable graphs, it's crucial to use this when
        # naming the CTEs so in subsequent checks we can match up all joinable fields,
        # not just fields in the same view.

        # Here we're checking if the field is a measure, if so we need to get the canon date
        # to calculate the join hash. This solves the issue where the canon date has a different
        # join hash from its associated metric. In that case we use the canon date's join hash.
        if field.field_type == "measure" and field.canon_date is not None:
            field = self.project.get_field_by_name(field.canon_date)
        joinable_graphs = [jg for jg in field.join_graphs() if "merged_result" not in jg]
        return "_".join(joinable_graphs)

    def _join_hash_contains_join_graph(self, join_hash: str, join_graphs: list):
        # Due to situations like subquery_1 and subquery_12 we have to split these
        # apart and check if any of the join graphs are in the split list
        join_hash_subqueries = []
        for query_number in join_hash.split("subquery"):
            clean_query_number = query_number.strip("_")
            if clean_query_number.isdigit():
                join_hash_subqueries.append(f"subquery_{clean_query_number}")
        return any(jg in join_hash_subqueries for jg in join_graphs)

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
