from copy import deepcopy

from metrics_layer.core.exceptions import QueryError, JoinError
from metrics_layer.core.sql.single_query_resolve import SingleSQLQueryResolver
from metrics_layer.core.sql.merged_query_resolve import MergedSQLQueryResolver
from metrics_layer.core.sql.query_base import QueryKindTypes


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
        model=None,
        **kwargs,
    ):
        self.field_lookup = {}
        self.no_group_by = False
        self.verbose = kwargs.get("verbose", False)
        self.select_raw_sql = kwargs.get("select_raw_sql", [])
        self.explore_name = kwargs.get("explore_name")
        self.suppress_warnings = kwargs.get("suppress_warnings", False)
        self.limit = kwargs.get("limit")
        self.single_query = kwargs.get("single_query", False)
        self.project = project
        self.metrics = metrics
        self.dimensions = dimensions
        self.funnel = funnel
        self.where = where
        self.having = having
        self.order_by = order_by
        self.kwargs = kwargs
        self.model = model
        self._resolve_mapped_fields()

    @property
    def is_merged_result(self):
        has_explicit_merge = any(self.project.get_field(m).is_merged_result for m in self.metrics)
        has_specified_merge = self.kwargs.get("merged_result", False)
        return has_explicit_merge or has_specified_merge

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
        self.mapping_lookup, self.field_lookup = {}, {}
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
                self.field_lookup[field_name] = self.project.get_field(
                    field_name, model=self.model
                ).join_graphs()

        if not self.mapping_lookup:
            return

        if self.field_lookup:
            mergeable_graphs, joinable_graphs = self._join_graphs_by_type(self.field_lookup)
            for name, mapped_field in self.mapping_lookup.items():
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
                    replace_with = self.determine_field_to_replace_with(
                        mapped_field, joinable_graphs, mergeable_graphs
                    )
                self._replace_mapped_field(name, replace_with)

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
