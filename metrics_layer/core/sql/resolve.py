from collections import Counter, defaultdict
from copy import deepcopy
from typing import List, Union

from metrics_layer.core.exceptions import JoinError, QueryError
from metrics_layer.core.model.filter import Filter, MetricsLayerFilterExpressionType
from metrics_layer.core.model.project import Project
from metrics_layer.core.sql.merged_query_resolve import MergedSQLQueryResolver
from metrics_layer.core.sql.query_base import QueryKindTypes
from metrics_layer.core.sql.single_query_resolve import SingleSQLQueryResolver


class SQLQueryResolver(SingleSQLQueryResolver):
    def __init__(
        self,
        metrics: list,
        dimensions: list = [],
        funnel: dict = {},  # A dict with steps (list) and within (dict)
        where: Union[str, None, List] = None,  # Either a list of json or a string
        having: Union[str, None, List] = None,  # Either a list of json or a string
        order_by: Union[str, None, List] = None,  # Either a list of json or a string
        project: Union[Project, None] = None,
        connections: List = [],
        **kwargs,
    ):
        self.field_lookup = {}
        self.no_group_by = False
        self.mapping_forces_merged_result = False
        self.verbose = kwargs.get("verbose", False)
        self.select_raw_sql = kwargs.get("select_raw_sql", [])
        self.suppress_warnings = kwargs.get("suppress_warnings", False)
        self.limit = kwargs.get("limit")
        self.single_query = kwargs.get("single_query", False)
        self.kwargs = kwargs
        self.project = project
        if self.kwargs.get("topic"):
            self.topic = self.project.get_topic(kwargs["topic"])
            self.kwargs["topic"] = self.topic
        else:
            self.topic = None
        self.model = self._get_model_for_query(kwargs.get("model_name"), metrics, dimensions)
        self.connections = connections
        self.metrics = metrics
        self.dimensions = dimensions
        self.funnel = funnel
        self.where = where if where else []
        always_where = self._apply_always_filter(metrics + dimensions)
        if always_where:
            self.where.extend(always_where)
        self.where = self._clean_conditional_filter_syntax(self.where)
        self.having = self._clean_conditional_filter_syntax(having)
        self.order_by = order_by
        self.connection = self._get_connection(self.model.connection)
        self.kwargs["query_type"] = self._get_query_type(self.connection, self.kwargs)
        connection_schema = self._get_connection_schema(self.connection)
        self.project.set_connection_schema(connection_schema)
        self.field_id_mapping = {}
        self._resolve_mapped_fields()
        self.query_type = None

    @property
    def is_merged_result(self):
        has_explicit_merge = any(self.project.get_field(m).is_merged_result for m in self.metrics)
        has_specified_merge = self.kwargs.get("merged_result", False)
        return has_explicit_merge or has_specified_merge or self.mapping_forces_merged_result

    def get_query(self, semicolon: bool = True):
        err_msg = ""
        is_explicit_merge = self.is_merged_result
        single_error = None
        if not is_explicit_merge:
            try:
                self.query_kind = QueryKindTypes.single
                return self._get_single_query(semicolon=semicolon)
            except JoinError as e:
                single_error = e
                err_msg = "Could not execute the query as a single query. Trying as a merged result query."
                if self.single_query:
                    raise e
                if self.verbose:
                    print(err_msg)
        self.query_kind = QueryKindTypes.merged
        try:
            return self._get_merged_result_query(semicolon=semicolon)
        except Exception as merged_error:
            if single_error and single_error.location == "topic":
                raise single_error

            appended = (
                "Zenlytic tries to merge query results by default if there is no join path between "
                "the views. If you'd like to disable this behavior pass single_query=True to the "
                "function call.\n\nIf you're seeing this and you expected the views to join on a "
                "primary or foreign key, make sure you have the right identifiers set on the views."
            )
            error_message = (
                f"{err_msg}\n\n{appended}\n\n{deepcopy(str(merged_error))}"
                if self.verbose
                else f"{deepcopy(str(merged_error))}"
            )
            raise merged_error

    def _get_single_query(self, semicolon: bool):
        resolver = SingleSQLQueryResolver(
            metrics=self.metrics,
            dimensions=self.dimensions,
            funnel=self.funnel,
            where=self.where,
            having=self.having,
            order_by=self.order_by,
            model=self.model,
            project=self.project,
            **self.kwargs,
        )
        query = resolver.get_query(semicolon)
        self.query_type = resolver.query_type
        return query

    def _get_merged_result_query(self, semicolon: bool):
        resolver = MergedSQLQueryResolver(
            metrics=self.metrics,
            dimensions=self.dimensions,
            funnel=self.funnel,
            where=self.where,
            having=self.having,
            order_by=self.order_by,
            model=self.model,
            project=self.project,
            **self.kwargs,
        )
        query = resolver.get_query(semicolon)
        self.query_type = resolver.query_type
        return query

    def _resolve_mapped_fields(self):
        self.mapping_lookup, self.field_lookup, self.field_object_lookup = {}, {}, {}
        self._where_fields, self._having_fields, self._order_fields = self.parse_field_names(
            self.where, self.having, self.order_by
        )
        self._all_fields = (
            self.metrics + self.dimensions + self._where_fields + self._having_fields + self._order_fields
        )

        replace_bools = [True] * len(self._all_fields) + [False] * len(
            self.kwargs.get("mapping_lookup_dimensions", [])
        )
        lookup_only_fields = [
            d for d in self.kwargs.get("mapping_lookup_dimensions", []) if d not in self._all_fields
        ]
        field_ids = self._all_fields + lookup_only_fields
        topic_join_graphs = self.topic.join_graphs() if self.topic else []
        for do_replace, field_name in zip(replace_bools, field_ids):
            mapped_field = self.project.get_mapped_field(field_name, model=self.model)
            if mapped_field:
                self.mapping_lookup[field_name] = {**mapped_field, "do_replace": do_replace}
            else:
                field_obj = self.project.get_field(field_name, model_name=self.model.name)
                self.field_object_lookup[field_name] = field_obj
                self.field_lookup[field_name] = field_obj.join_graphs() + topic_join_graphs

        if not self.mapping_lookup:
            # We need to call this here because the 'field' value in the
            # group by filter does not show up in the field lookup
            self._replace_field_value_in_group_by_filter()
            return

        if self.field_lookup:
            mergeable_graphs, joinable_graphs = self._join_graphs_by_type(self.field_lookup)
            self._handle_invalid_merged_result(mergeable_graphs, joinable_graphs)
            for name, mapped_field in self.mapping_lookup.items():
                if not mapped_field.pop("do_replace"):
                    continue  # Skip fields that are only for lookup

                is_date_mapping = mapped_field["name"] in self.model.special_mapping_values
                if is_date_mapping and len(self.metrics) >= 1:
                    # Use the first metric to replace the mapped field everywhere
                    # This is fine to do in the case we have multiple metrics because in that
                    # case we'll need to run a merged result query which will take care of mapping the
                    # canon_date's for each metric regardless of the canon_date chosen for the filters
                    first_metric_field = self._get_field_from_lookup(self.metrics[0])
                    canon_date_id = f'{first_metric_field.canon_date}_{mapped_field["name"]}'
                    replace_with = self.project.get_field(canon_date_id)

                    # If the additional metrics have different canon_dates, we'll need to
                    # change the query type to be a merged result
                    for field_name in self.metrics[1:]:
                        metric_field = self._get_field_from_lookup(field_name)
                        if metric_field.canon_date != first_metric_field.canon_date:
                            self.mapping_forces_merged_result = True
                            break
                elif is_date_mapping and len(self.dimensions) >= 1:
                    canon_dates = set()
                    for d in self.dimensions:
                        field = self._get_field_from_lookup(d, only_search_lookup=True)
                        if field and field.canon_date:
                            canon_dates.add(field.canon_date)

                    if len(canon_dates) >= 1:
                        # When there is no measure present, we will chose the canon date
                        # from the dimension that has the most measures
                        if len(canon_dates) > 1:
                            views_by_n_measures = {}
                            for canon_date in canon_dates:
                                view_name = canon_date.split(".")[0]
                                measures = [
                                    f
                                    for f in self.project.get_view(view_name).fields()
                                    if f.field_type == "measure"
                                ]
                                views_by_n_measures[canon_date] = len(measures)
                            sorted_canon_dates = list(
                                sorted(canon_dates, key=lambda x: views_by_n_measures[x], reverse=True)
                            )
                        else:
                            # Sort is not needed if there is only one canon_date
                            sorted_canon_dates = list(canon_dates)

                        canon_date_id = f'{sorted_canon_dates[0]}_{mapped_field["name"]}'
                        replace_with = self.project.get_field(canon_date_id)
                    else:
                        replace_with = self.determine_field_to_replace_with(
                            mapped_field, joinable_graphs, mergeable_graphs
                        )
                else:
                    replace_with = self.determine_field_to_replace_with(
                        mapped_field, joinable_graphs, mergeable_graphs
                    )
                self._replace_mapped_field(name, replace_with)
        else:
            # This is the scenario where we only have mappings and no other fields in the query

            # First, we need to get all join graphs for all fields involved in the mappings.
            # Since we have no "real" fields, we have no basis for required join graphs we
            # must match, yet. This gives us the raw material to derive which join graphs
            # overlap in the mappings.
            check = defaultdict(list)
            for name, mapped_field in self.mapping_lookup.items():
                for field_id in mapped_field["fields"]:
                    ref_field = self.project.get_field(field_id)
                    check[name].append((field_id, ref_field.join_graphs()))

            # Next, we iterate over all mappings and check if there is a valid join path
            # between all fields in the mappings.
            validity = defaultdict(dict)
            # This is done my comparing a single mapping (name), then...
            for name, field_info in check.items():
                # Looking at all fields that are present in that mapping name, and
                # comparing them to all other fields in all other mappings
                for field_id, join_graphs in field_info:
                    passed, points = 0, 0
                    for other_name, other_field_info in check.items():
                        if name != other_name:
                            for other_field_id, other_join_graphs in other_field_info:
                                if field_id != other_field_id:
                                    # If the fields are joinable or mergeable, then we can form a
                                    # valid query, and we can count them as 'passed'
                                    if set(join_graphs).intersection(set(other_join_graphs)):
                                        passed += 1
                                        # It's not enough just to pass though. There are some things we prefer,
                                        # when there is no solid direction on which fields to choose.

                                        # 1. We prefer when fields are in the same view
                                        points += (
                                            1 if field_id.split(".")[0] == other_field_id.split(".")[0] else 0
                                        )

                                        # 2. We prefer when fields can be joined over when they can only be merged
                                        joinable_first = [j for j in join_graphs if "merged_result" not in j]
                                        joinable_second = [
                                            j for j in other_join_graphs if "merged_result" not in j
                                        ]
                                        if set(joinable_first).intersection(set(joinable_second)):
                                            points += 1
                                        # Once we have determined a field pair is valid,
                                        # we can stop checking other fields under that mapping name
                                        break

                    # If at least one field in each of the other mapping names has 'passed' the check,
                    # then we can consider this mapping name as valid, and assign it the points
                    if passed == len(check) - 1:
                        validity[name][field_id] = points

            # If this exists and we have more than one mapping present
            if validity and len(self.mapping_lookup) > 1:
                replaced_mapping = None
                all_items = [
                    (name, *item) for name, field_info in validity.items() for item in field_info.items()
                ]
                # Sort the mappings by points, and then by field_id (so it is consistent in
                # the event fields have the same points), and replace the highest ranking one
                # and set its join graphs as the active ones all other mappings must adhere to.
                for name, field_id, points in sorted(all_items, key=lambda x: (x[-1], x[1]), reverse=True):
                    replaced_mapping = name
                    replace_with = self.project.get_field(field_id)
                    self.field_lookup[name] = replace_with.join_graphs()
                    break

                self._replace_mapped_field(name, replace_with)
            # In the event there is only one mapping, just pick the first field in the options
            elif len(self.mapping_lookup) == 1:
                for name, mapped_field in self.mapping_lookup.items():
                    replaced_mapping = name
                    replace_with = self.project.get_field(mapped_field["fields"][0])
                    self.field_lookup[name] = replace_with.join_graphs()
                self._replace_mapped_field(name, replace_with)
            else:
                raise QueryError("No valid join path found for mapped fields")

            for name, mapped_field in self.mapping_lookup.items():
                if name != replaced_mapping:
                    mergeable_graphs, joinable_graphs = self._join_graphs_by_type(self.field_lookup)
                    self._handle_invalid_merged_result(mergeable_graphs, joinable_graphs)
                    replace_with = self.determine_field_to_replace_with(
                        mapped_field, joinable_graphs, mergeable_graphs
                    )
                    self._replace_mapped_field(name, replace_with)

        # We also have to swap out the field in a potential group by where clause
        self._replace_field_value_in_group_by_filter()

    def _replace_field_value_in_group_by_filter(self):
        if isinstance(self.where, list) and self.field_lookup:
            optimal_join_graph_connection = set.intersection(*map(set, self.field_lookup.values()))
            optimal_join_graph_connection = [
                o for o in optimal_join_graph_connection if "merged_result" not in o
            ]
            flattened_conditions = SingleSQLQueryResolver.flatten_filters(
                self.where, return_nesting_depth=True
            )
            for cond in flattened_conditions:
                if "group_by" in cond:
                    # Only the group by field needs to be joinable or merge-able to the query
                    # The field in the group by where clause is not required to be joinable
                    # to the whole query, just to the group by field
                    group_by_field = self.project.get_field(cond["group_by"], model_name=self.model.name)
                    join_graphs = group_by_field.join_graphs()

                    # Here we need to check if the field is a mapped field
                    # If it is, we need to add the underlying field
                    mapped_field = self.project.get_mapped_field(cond["field"], model=self.model)
                    if mapped_field:
                        replace_with = self.determine_field_to_replace_with(
                            mapped_field, optimal_join_graph_connection, join_graphs
                        )
                        self.field_id_mapping[cond["field"]] = replace_with.id()
                        cond["field"] = replace_with.id()
                elif cond["expression"] in {
                    MetricsLayerFilterExpressionType.IsInQuery.value,
                    MetricsLayerFilterExpressionType.IsNotInQuery.value,
                }:
                    defaults = {
                        "project": self.project,
                        "connections": self.connections,
                        "model_name": self.model.name,
                        "return_pypika_query": False,
                    }
                    if "query_type" in self.kwargs:
                        defaults["query_type"] = self.kwargs["query_type"]

                    # This handles the case where the passed filter is incomplete, and
                    # does not apply the filter
                    if "query" not in cond["value"]:
                        continue

                    if "query" in cond["value"] and not isinstance(cond["value"]["query"], dict):
                        raise QueryError(
                            "Subquery filter value for the key 'query' must be a dictionary. It was"
                            f" {cond['value']['query']}"
                        )

                    if "apply_limit" in cond["value"] and not bool(cond["value"]["apply_limit"]):
                        cond["value"]["query"]["limit"] = None

                    if "nesting_depth" in cond and cond["nesting_depth"] > 0:
                        defaults["nesting_depth"] = cond["nesting_depth"]

                    resolver = SQLQueryResolver(**cond["value"]["query"], **defaults)
                    jg_connection = set.intersection(*map(set, resolver.field_lookup.values()))
                    optimal_jg_connection = [o for o in jg_connection if "merged_result" not in o]

                    mapped_field = self.project.get_mapped_field(cond["value"]["field"], model=self.model)
                    if mapped_field:
                        field = self.determine_field_to_replace_with(
                            mapped_field, optimal_jg_connection, jg_connection
                        )
                        self.field_id_mapping[cond["value"]["field"]] = field.id()
                        cond["value"]["field"] = field.id()
                    else:
                        field = self.project.get_field(cond["value"]["field"])
                    if field.id() not in {self.project.get_field(d).id() for d in resolver.dimensions}:
                        raise QueryError(
                            f"Field {field.id()} not found in subquery dimensions {resolver.dimensions}. You"
                            " must specify a dimension that is present in the subquery."
                        )
                    cond["value"]["sql_query"] = resolver.get_query(semicolon=False)

    def _get_field_from_lookup(self, field_name: str, only_search_lookup: bool = False):
        if field_name in self.field_object_lookup:
            metric_field = self.field_object_lookup[field_name]
        elif only_search_lookup:
            return None
        else:
            metric_field = self.project.get_field(field_name, model_name=self.model.name)
        return metric_field

    def determine_field_to_replace_with(self, mapped_field, joinable_graphs, mergeable_graphs):
        joinable, mergeable = [], []
        for field_name in mapped_field["fields"]:
            field = self.project.get_field(field_name)
            join_graphs = field.join_graphs()
            if any(g in joinable_graphs for g in join_graphs):
                joinable.append(field)
            elif any(g in mergeable_graphs for g in join_graphs):
                mergeable.append(field)

        if joinable:
            return joinable[0]
        elif mergeable:
            return mergeable[0]
        else:
            raise QueryError(f'No valid join path found for mapped field "{mapped_field["name"]}"')

    def _join_graphs_by_type(self, field_lookup: dict):
        usable_merged_graphs = set.intersection(*map(set, field_lookup.values()))
        usable_joinable_graphs = [s for s in list(usable_merged_graphs) if "merged_result" not in s]
        return usable_merged_graphs, usable_joinable_graphs

    def _handle_invalid_merged_result(self, mergeable_graphs, joinable_graphs):
        # If both of these are empty there is no join overlap and the query cannot be run
        if len(mergeable_graphs) == 0 and len(joinable_graphs) == 0:
            # first we try to find the issue by removing one key from the lookup and seeing
            # if it works with the rest. This produces the most sensible error message.
            for key in self.field_lookup.keys():
                with_one_removed = {k: v for k, v in self.field_lookup.items() if k != key}
                merged, joined = self._join_graphs_by_type(with_one_removed)
                if len(merged) != 0 or len(joined) != 0:
                    error_message = (
                        f"The field {key} could not be either joined into the query "
                        "or mapped and merged into the query as a merged result. \n\n"
                        "Check that you specify joins to join it in, or specify a mapping "
                        "for a query with two tables that cannot be merged"
                        if self.verbose
                        else (
                            f"Error: The field {key} could not be joined or mapped and merged into the query."
                            " Please try to reformat the query. If the error persists, consult the user for"
                            " further guidance."
                        )
                    )
                    raise QueryError(error_message)
            # Otherwise, we have to show this, worse, error message
            all_fields = list(self.field_lookup.keys()) + list(self.mapping_lookup.keys())
            error_message = (
                "The query could not be either joined or mapped and merged into a valid query"
                f" with the fields:\n\n{', '.join(all_fields)}\n\n"
                "Check that those fields can be joined together or are mapped so they can "
                "be merged across tables"
                if self.verbose
                else (
                    "Error: The query could not be either joined or mapped and merged into a valid query"
                    f" with the fields:\n\n{', '.join(all_fields)}\n\n Please try to reformat the query. If"
                    " the error persists, consult the user for further guidance."
                )
            )
            raise QueryError(error_message)

    def _replace_mapped_field(self, to_replace: str, field):
        self.field_id_mapping[to_replace] = field.id()
        if to_replace in self.metrics:
            idx = self.metrics.index(to_replace)
            self.metrics[idx] = field.id()
        if to_replace in self.dimensions:
            idx = self.dimensions.index(to_replace)
            self.dimensions[idx] = field.id()
        if to_replace in self._where_fields:
            self.where = self._replace_dict_or_literal(self.where, to_replace, field)
        if to_replace in self._having_fields:
            self.having = self._replace_dict_or_literal(self.having, to_replace, field)
        if to_replace in self._order_fields:
            self.order_by = self._replace_dict_or_literal(self.order_by, to_replace, field)

        if to_replace not in self._all_fields:
            raise QueryError(f"Could not find mapped field {to_replace} in query")

    def _replace_dict_or_literal(self, where, to_replace, field):
        if self._is_literal(where):
            return where.replace(to_replace, field.id())
        else:
            result = []
            for w in where:
                if "group_by" in w and w["group_by"] == to_replace:
                    result.append({**w, "group_by": field.id()})
                elif "field" in w and w["field"] == to_replace and "value" in w and w["value"] == to_replace:
                    result.append({**w, "field": field.id(), "value": field.id()})
                elif "value" in w and w["value"] == to_replace:
                    result.append({**w, "value": field.id()})
                elif "field" in w and w["field"] == to_replace:
                    result.append({**w, "field": field.id()})
                elif "field" not in w and "conditions" in w:
                    result.append(
                        {**w, "conditions": self._replace_dict_or_literal(w["conditions"], to_replace, field)}
                    )
                else:
                    result.append(w)
            return result

    def _get_model_for_query(self, model_name: str = None, metrics: list = [], dimensions: list = []):
        models = self.project.models()
        # If you specify the model that's top priority
        if model_name:
            return self.project.get_model(model_name)
        # Otherwise, if there's only one option, we use that
        elif len(models) == 1:
            return models[0]
        # Raise an error if the user doesn't have any models defined yet
        elif len(models) == 0:
            raise QueryError(
                "No models found in this data model. Please specify a model "
                "to connect a data warehouse to your data model."
            )
        # Finally, check views for models
        else:
            return self._derive_model(metrics, dimensions)

    def _derive_model(self, metrics: list, dimensions: list):
        all_model_names, mapping_model_names = [], []
        models = self.project.models()
        for f in metrics + dimensions:
            try:
                model_name = self.project.get_field(f).view.model_name
                all_model_names.append(model_name)
            except Exception:
                for model in models:
                    try:
                        self.project.get_mapped_field(f, model=model)
                        mapping_model_names.append(model.name)
                    except Exception:
                        pass
        all_model_names = list(set(all_model_names))
        if not all_model_names:
            if mapping_model_names:
                # In this case, the only fields we recognize are mappings. These can
                # exist in multiple models, so we don't know which to use. Let's make a
                # guess and choose the model containing the most mappings.
                model_counts = Counter(mapping_model_names)
                sorted_models = [m for m, _ in model_counts.most_common()]
                return self.project.get_model(sorted_models[0])
            # Alternatively, we don't recognize any fields. Let's arbitrarily choose the
            # first model.
            return models[0]
        if len(all_model_names) == 1 and (
            len(mapping_model_names) == 0
            or (len(mapping_model_names) > 0 and all_model_names[0] in mapping_model_names)
        ):
            return self.project.get_model(list(all_model_names)[0])
        raise QueryError(
            "More than one model found in this query. Please specify a model to use by either passing the"
            " name of the model using 'model_name' parameter or by setting the `model_name` property on the"
            " view."
        )

    def _get_query_type(self, connection, kwargs: dict):
        if "query_type" in kwargs:
            return kwargs.pop("query_type")
        elif connection:
            return connection.type
        else:
            raise QueryError(
                "Could not determine query_type. Please have connection information for "
                "your warehouse in the configuration or explicitly pass the "
                "'query_type' argument to this function"
            )

    def _apply_always_filter(self, fields: list):
        always_where = []
        to_add = {"week_start_day": self.model.week_start_day, "timezone": self.project.timezone}
        for field_str in fields:
            try:
                field = self.project.get_field(field_str)
                if field.view.always_filter:
                    parsed_filters = []
                    for f in field.view.always_filter:
                        if "." not in f["field"]:
                            f["field"] = f"{field.view.name}.{f['field']}"
                        parsed_filters.extend(Filter({**f, **to_add}).filter_dict(json_safe=True))
                    always_where.extend(parsed_filters)
            # Handle mappings exception
            except Exception:
                pass

        # Apply always filters from the topic (if present)
        if self.topic and self.topic.always_filter:
            for f in self.topic.always_filter:
                if "." not in f["field"]:
                    raise QueryError(
                        f"Always filter field {f['field']} in the topic {self.topic.label} must be a fully"
                        " qualified field name in the format view_name.field_name"
                    )
                always_where.extend(Filter({**f, **to_add}).filter_dict(json_safe=True))

        return self._deduplicate_always_where_filters(always_where)

    @staticmethod
    def _deduplicate_always_where_filters(filters: list):
        seen = set()
        cleaned_filters = []
        for f in filters:
            hashable_filter = tuple((k, v if not isinstance(v, list) else tuple(v)) for k, v in f.items())
            if hashable_filter not in seen:
                seen.add(hashable_filter)
                cleaned_filters.append(f)
        return cleaned_filters

    def _clean_conditional_filter_syntax(self, filters: Union[str, None, List]):
        if not filters or isinstance(filters, str):
            return filters

        if isinstance(filters, dict):
            return [filters]

        def process_filter(filter_obj):
            if isinstance(filter_obj, dict):
                if "conditional_filter_logic" in filter_obj:
                    return filter_obj["conditional_filter_logic"]
                elif "conditions" in filter_obj:
                    filter_obj["conditions"] = [process_filter(cond) for cond in filter_obj["conditions"]]
            return filter_obj

        return [process_filter(filter_obj) for filter_obj in filters]

    def _get_connection_schema(self, connection):
        if connection is not None:
            return getattr(connection, "schema", None)
        return None

    def _get_connection(self, connection_name: str):
        return next((c for c in self.connections if c.name == connection_name), None)
