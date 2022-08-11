from metrics_layer.core.exceptions import QueryError, JoinError
from metrics_layer.core.parse.config import MetricsLayerConfiguration
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
        config: MetricsLayerConfiguration = None,
        **kwargs,
    ):
        self.field_lookup = {}
        self.no_group_by = False
        self.verbose = kwargs.get("verbose", False)
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

    @property
    def is_merged_result(self):
        has_explicit_merge = any(self.project.get_field(m).is_merged_result for m in self.metrics)
        has_specified_merge = self.kwargs.get("merged_result", False)
        return has_explicit_merge or has_specified_merge

    def get_query(self, semicolon: bool = True):
        is_explicit_merge = self.is_merged_result
        if not is_explicit_merge:
            try:
                self.query_kind = QueryKindTypes.single
                return self._get_single_query(semicolon=semicolon)
            except JoinError:
                if self.verbose:
                    print("Could not execute the query as a single query. Trying as a merged result query.")
        self.query_kind = QueryKindTypes.merged
        return self._get_merged_result_query(semicolon=semicolon)

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
        resolver = MergedSQLQueryResolver(
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
