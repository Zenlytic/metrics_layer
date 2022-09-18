from copy import deepcopy

from metrics_layer.core.exceptions import QueryError, JoinError
from metrics_layer.core.sql.single_query_resolve import SingleSQLQueryResolver
from metrics_layer.core.sql.merged_query_resolve import MergedSQLQueryResolver
from metrics_layer.core.sql.query_base import QueryKindTypes


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
