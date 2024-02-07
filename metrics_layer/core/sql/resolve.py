from copy import deepcopy

from metrics_layer.core.exceptions import QueryError, JoinError
from metrics_layer.core.sql.single_query_resolve import SingleSQLQueryResolver
from metrics_layer.core.sql.merged_query_resolve import MergedSQLQueryResolver
from metrics_layer.core.sql.query_base import QueryKindTypes
from metrics_layer.core.model.filter import Filter


class SQLQueryResolver(SingleSQLQueryResolver):
    def __init__(
        self,
        metrics: list,
        dimensions: list = [],
        funnel: dict = {},  # A dict with steps (list) and within (dict)
        where: str = None,  # Either a list of json or a string
        having: str = None,  # Either a list of json or a string
        order_by: str = None,  # Either a list of json or a string
        project=None,
        connections=[],
        **kwargs,
    ):
        self.field_lookup = {}
        self.no_group_by = False
        self.mapping_forces_merged_result = False
        self.verbose = kwargs.get("verbose", False)
        self.select_raw_sql = kwargs.get("select_raw_sql", [])
        self.explore_name = kwargs.get("explore_name")
        self.suppress_warnings = kwargs.get("suppress_warnings", False)
        self.limit = kwargs.get("limit")
        self.single_query = kwargs.get("single_query", False)
        self.project = project
        self.model = self._get_model_for_query(kwargs.get("model_name"), metrics, dimensions)
        self.connections = connections
        self.metrics = metrics
        self.dimensions = dimensions
        self.funnel = funnel
        self.where = where if where else []
        always_where = self._apply_always_filter(metrics + dimensions)
        if always_where:
            self.where.extend(always_where)
        self.having = having
        self.order_by = order_by
        self.kwargs = kwargs
        self.connection = self._get_connection(self.model.connection)
        self.kwargs["query_type"] = self._get_query_type(self.connection, self.kwargs)
        connection_schema = self._get_connection_schema(self.connection)
        self.project.set_connection_schema(connection_schema)
        self._resolve_mapped_fields()

    @property
    def is_merged_result(self):
        has_explicit_merge = any(self.project.get_field(m).is_merged_result for m in self.metrics)
        has_specified_merge = self.kwargs.get("merged_result", False)
        return has_explicit_merge or has_specified_merge or self.mapping_forces_merged_result

    def get_query(self, semicolon: bool = True):
        err_msg = ""
        is_explicit_merge = self.is_merged_result
        if not is_explicit_merge:
            try:
                self.query_kind = QueryKindTypes.single
                return self._get_single_query(semicolon=semicolon)
            except JoinError as e:
                err_msg = "Could not execute the query as a single query. Trying as a merged result query."
                if self.single_query:
                    raise e
                if self.verbose:
                    print(err_msg)
        self.query_kind = QueryKindTypes.merged
        try:
            return self._get_merged_result_query(semicolon=semicolon)
        except QueryError as e:
            appended = (
                "Zenlytic tries to merge query results by default if there is no join path between "
                "the views. If you'd like to disable this behavior pass single_query=True to the "
                "function call.\n\nIf you're seeing this and you expected the views to join on a "
                "primary or foreign key, make sure you have the right identifiers set on the views."
            )
            e.message = f"{err_msg}\n\n{appended} \n\n" + deepcopy(e.message)
            raise e

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
        return query

    def _resolve_mapped_fields(self):
        self.mapping_lookup, self.field_lookup, self.field_object_lookup = {}, {}, {}
        self._where_fields, self._having_fields, self._order_fields = self.parse_field_names(
            self.where, self.having, self.order_by
        )
        self._all_fields = (
            self.metrics + self.dimensions + self._where_fields + self._having_fields + self._order_fields
        )
        for field_name in self._all_fields:
            mapped_field = self.project.get_mapped_field(field_name, model=self.model)
            if mapped_field:
                self.mapping_lookup[field_name] = mapped_field
            else:
                field_obj = self.project.get_field(field_name, model=self.model)
                self.field_object_lookup[field_name] = field_obj
                self.field_lookup[field_name] = field_obj.join_graphs()

        if not self.mapping_lookup:
            return

        if self.field_lookup:
            mergeable_graphs, joinable_graphs = self._join_graphs_by_type(self.field_lookup)
            self._handle_invalid_merged_result(mergeable_graphs, joinable_graphs)
            for name, mapped_field in self.mapping_lookup.items():
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
            for i, (name, mapped_field) in enumerate(self.mapping_lookup.items()):
                if i == 0:
                    replace_with = self.project.get_field(mapped_field["fields"][0])
                    self.field_lookup[name] = replace_with.join_graphs()
                else:
                    mergeable_graphs, joinable_graphs = self._join_graphs_by_type(self.field_lookup)
                    self._handle_invalid_merged_result(mergeable_graphs, joinable_graphs)
                    replace_with = self.determine_field_to_replace_with(
                        mapped_field, joinable_graphs, mergeable_graphs
                    )
                self._replace_mapped_field(name, replace_with)

    def _get_field_from_lookup(self, field_name: str, only_search_lookup: bool = False):
        if field_name in self.field_object_lookup:
            metric_field = self.field_object_lookup[field_name]
        elif only_search_lookup:
            return None
        else:
            metric_field = self.project.get_field(field_name, model=self.model)
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
                    raise QueryError(
                        f"The field {key} could not be either joined into the query "
                        "or mapped and merged into the query as a merged result. \n\n"
                        "Check that you specify joins to join it in, or specify a mapping "
                        "for a query with two tables that cannot be merged"
                    )
            # Otherwise, we have to show this, worse, error message
            all_fields = list(self.field_lookup.keys()) + list(self.mapping_lookup.keys())
            raise QueryError(
                f"The query could not be either joined or mapped and merged into a valid query"
                f" with the fields:\n\n{', '.join(all_fields)}\n\n"
                "Check that those fields can be joined together or are mapped so they can "
                "be merged across tables"
            )

    def _replace_mapped_field(self, to_replace: str, field):
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
            return [{**w, "field": field.id()} if w["field"] == to_replace else w for w in where]

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
        all_model_names = []
        models = self.project.models()
        for f in metrics + dimensions:
            try:
                model_name = self.project.get_field(f).view.model_name
                all_model_names.append(model_name)
            except Exception:
                for model in models:
                    try:
                        self.project.get_mapped_field(f, model=model)
                        all_model_names.append(model.name)
                        break
                    except Exception:
                        pass

        all_model_names = list(set(all_model_names))

        if len(all_model_names) == 0:
            # In a case that there are no models in the query, we'll just use the first model
            # in the project. This case should be limited to only mapping-only queries, so this is safe.
            return self.project.models()[0]
        elif len(all_model_names) == 1:
            return self.project.get_model(list(all_model_names)[0])
        else:
            raise QueryError(
                "More than one model found in this query. Please specify a model "
                "to use by either passing the name of the model using 'model_name' parameter or by  "
                "setting the `model_name` property on the view."
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

    def _get_connection_schema(self, connection):
        if connection is not None:
            return getattr(connection, "schema", None)
        return None

    def _get_connection(self, connection_name: str):
        return next((c for c in self.connections if c.name == connection_name), None)
