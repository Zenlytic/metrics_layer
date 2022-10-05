import sqlparse

from metrics_layer.core.convert import MQLConverter
from metrics_layer.core.parse import ProjectLoader
from metrics_layer.core.parse.connections import BaseConnection
from metrics_layer.core.sql import QueryRunner, SQLQueryResolver
from metrics_layer.core.sql.query_errors import ParseError
from metrics_layer.core.exceptions import QueryError


class DBConnectionError(Exception):
    pass


class MetricsLayerConnection:
    def __init__(
        self,
        location: str = None,
        branch: str = "master",
        project=None,
        connections: list = [],
        user: dict = None,
        **kwargs,
    ):
        self.location, self.branch, self._raw_connections = location, branch, connections
        self._raw_project = project
        self.kwargs = kwargs
        self._user = user
        self.branch_options = None
        self._project = None

    def set_user(self, user: dict):
        self._user = user
        self.project.set_user(self._user)

    def load(self):
        if self.location is not None:
            self._loader = ProjectLoader(self.location, self.branch, self._raw_connections)
            self._project = self._loader.load()
            self._project.set_user(self._user)
            self.branch_options = self._loader.get_branch_options()
        elif self._raw_project is not None:
            self._project = self._raw_project
            self._project.set_user(self._user)
            self.branch_options = []
        else:
            raise DBConnectionError(
                "No project or location specified. Pass either the location "
                "(a path or a github url) or a project object."
            )

    @property
    def profiles_path(self):
        return ProjectLoader.profiles_path()

    @property
    def project(self):
        if self._project is None:
            self.load()
        return self._project

    def get_branch_options(self):
        if self.branch_options is not None:
            return self.branch_options
        self.load()
        return self.branch_options

    def add_connections(self, additional_raw_connections: list):
        self._raw_connections = self._raw_connections + additional_raw_connections

    @property
    def connections(self):
        self._connections = ProjectLoader.load_connections(self._raw_connections)
        return self._connections

    def list_connections(self, names_only=False):
        connections = self.connections
        if names_only:
            return [c.name for c in connections]
        return connections

    def get_connection(self, connection_name: str):
        return next((c for c in self.list_connections() if c.name == connection_name), None)

    def query(
        self,
        metrics: list = [],
        dimensions: list = [],
        funnel: dict = {},
        where: list = [],
        having: list = [],
        order_by: list = [],
        sql: str = None,
        **kwargs,
    ):
        query, connection = self.get_sql_query(
            sql=sql,
            metrics=metrics,
            dimensions=dimensions,
            funnel=funnel,
            where=where,
            having=having,
            order_by=order_by,
            **{**self.kwargs, **kwargs},
            return_connection=True,
        )
        df = self.run_query(query, connection, **kwargs)
        return df

    def get_sql_query(
        self,
        metrics: list = [],
        dimensions: list = [],
        funnel: dict = {},
        where: list = [],
        having: list = [],
        order_by: list = [],
        sql: str = None,
        **kwargs,
    ):
        model = self._get_model_for_query(kwargs.get("model_name"), metrics, dimensions)
        connection = self.get_connection(model.connection)
        query_type = self._get_query_type(connection, kwargs)
        connection_schema = self._get_connection_schema(connection)
        self.project.set_connection_schema(connection_schema)
        if sql:
            converter = MQLConverter(
                sql,
                project=self.project,
                model=model,
                query_type=query_type,
                **{**self.kwargs, **kwargs},
            )
            query = converter.get_query()
        else:
            resolver = SQLQueryResolver(
                metrics=metrics,
                dimensions=dimensions,
                funnel=funnel,
                where=where,
                having=having,
                order_by=order_by,
                project=self.project,
                model=model,
                query_type=query_type,
                **{**self.kwargs, **kwargs},
            )
            query = resolver.get_query()

        if kwargs.get("pretty", False):
            query = self.pretty_sql(query)

        if kwargs.get("return_connection", False):
            return query, connection

        if kwargs.get("return_query_kind", False):
            return query, resolver.query_kind
        return query

    def run_query(self, query: str, connection: BaseConnection, **kwargs):
        runner = QueryRunner(query, connection)
        df = runner.run_query(**{**self.kwargs, **kwargs})
        return df

    def define(self, metric: str, **kwargs):
        field = self.project.get_field(metric)
        connection = self.get_connection(field.view.model.connection)
        query_type = self._get_query_type(connection, kwargs)
        return field.sql_query(query_type)

    def list_fields(self, view_name: str = None, names_only: bool = False, show_hidden: bool = False):
        all_fields = self.project.fields(view_name=view_name, show_hidden=show_hidden)
        if names_only:
            return [m.name for m in all_fields]
        return all_fields

    def get_field(self, field_name: str, view_name: str = None):
        models = self.list_models()
        kws = {}
        if len(models) == 1:
            kws = {"model": models[0]}
        return self.project.get_field(field_name, view_name=view_name, **kws)

    def list_metrics(self, view_name: str = None, names_only: bool = False, show_hidden: bool = False):
        all_fields = self.project.fields(view_name=view_name, show_hidden=show_hidden)
        metrics = [f for f in all_fields if f.field_type == "measure"]
        if names_only:
            return [m.name for m in metrics]
        return metrics

    def get_metric(self, metric_name: str, view_name: str = None):
        metrics = self.list_metrics(view_name=view_name)
        try:
            metric = next((m for m in metrics if m.equal(metric_name)))
            return metric
        except ParseError as e:
            raise e(f"Could not find metric {metric_name} in the project config")

    def list_dimensions(self, view_name: str = None, names_only: bool = False, show_hidden: bool = False):
        all_fields = self.project.fields(view_name=view_name, show_hidden=show_hidden)
        dimensions = [f for f in all_fields if f.field_type in {"dimension", "dimension_group"}]
        if names_only:
            return [d.name for d in dimensions]
        return dimensions

    def get_dimension(self, dimension_name: str, view_name: str = None):
        dimensions = self.list_dimensions(view_name=view_name)
        try:
            dimension = next((d for d in dimensions if d.equal(dimension_name)))
            return dimension
        except ParseError as e:
            raise e(f"Could not find dimension {dimension_name} in the project config")

    def list_views(self, names_only=False):
        views = self.project.views()
        if names_only:
            return [v.name for v in views]
        return views

    def get_view(self, view_name: str):
        return self.project.get_view(view_name)

    def list_models(self, names_only=False):
        models = self.project.models()
        if names_only:
            return [m.name for m in models]
        return models

    def get_model(self, model_name: str):
        return self.project.get_model(model_name)

    def list_dashboards(self, names_only=False):
        dashboards = self.project.dashboards()
        if names_only:
            return [d.name for d in dashboards]
        return dashboards

    def get_dashboard(self, dashboard_name: str):
        return self.project.get_dashboard(dashboard_name)

    def get_all_profiles(self, names_only: bool = False):
        raise NotImplementedError()

    def _get_model_for_query(self, model_name: str = None, metrics: list = [], dimensions: list = []):
        models = self.project.models()

        # If you specify the model that's top priority
        if model_name:
            return self.project.get_model(model_name)
        # Otherwise, if there's only one option, we use that
        elif len(models) == 1:
            return models[0]
        # Finally, check views for models
        else:
            return self._derive_model(metrics, dimensions)

    def _derive_model(self, metrics: list, dimensions: list):
        all_model_names = {f.view.model_name for f in metrics + dimensions}

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

    def _get_connection_schema(self, connection):
        if connection is not None:
            return getattr(connection, "schema", None)
        return None

    @staticmethod
    def get_connections_from_profile(profile_name: str):
        return ProjectLoader.get_connections_from_profile(profile_name)

    @staticmethod
    def pretty_sql(sql: str, keyword_case="lower"):
        return sqlparse.format(sql, reindent=True, keyword_case=keyword_case)
