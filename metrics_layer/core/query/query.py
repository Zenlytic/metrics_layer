import sqlparse
from typing import Union

from metrics_layer.core.convert import MQLConverter
from metrics_layer.core.parse import MetricsLayerConfiguration
from metrics_layer.core.parse.connections import BaseConnection
from metrics_layer.core.sql import QueryRunner, SQLQueryResolver
from metrics_layer.core.sql.query_errors import ParseError


class MetricsLayerConnection:
    def __init__(
        self,
        config: Union[MetricsLayerConfiguration, str, dict] = None,
        target: str = None,
        user: dict = None,
        **kwargs,
    ):
        if isinstance(config, str):
            self.config = MetricsLayerConfiguration(config, target=target)
        elif isinstance(config, dict):
            connections = config.pop("connections", [])
            self.config = MetricsLayerConfiguration(config, connections=connections, target=target)
        else:
            self.config = MetricsLayerConfiguration.get_metrics_layer_configuration(config, target=target)
        self.kwargs = kwargs
        self._user = user

    def set_user(self, user: dict):
        self._user = user
        self.config.set_user(user)

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
        if sql:
            converter = MQLConverter(sql, config=self.config, **{**self.kwargs, **kwargs})
            query = converter.get_query()
            connection = converter.connection
        else:
            resolver = SQLQueryResolver(
                metrics=metrics,
                dimensions=dimensions,
                funnel=funnel,
                where=where,
                having=having,
                order_by=order_by,
                config=self.config,
                **{**self.kwargs, **kwargs},
            )
            query = resolver.get_query()
            connection = resolver.connection

        if kwargs.get("pretty", False):
            query = self.pretty_sql(query)

        if kwargs.get("return_connection", False):
            return query, connection
        return query

    def run_query(self, query: str, connection: BaseConnection, **kwargs):
        runner = QueryRunner(query, connection)
        df = runner.run_query(**{**self.kwargs, **kwargs})
        return df

    def define(self, metric: str, query_type: str = None):
        field = self.config.project.get_field(metric)
        return field.sql_query(query_type)

    def list_fields(
        self,
        view_name: str = None,
        names_only: bool = False,
        show_hidden: bool = False,
    ):
        all_fields = self.config.project.fields(view_name=view_name, show_hidden=show_hidden)
        if names_only:
            return [m.name for m in all_fields]
        return all_fields

    def get_field(self, field_name: str, view_name: str = None):
        models = self.list_models()
        kws = {}
        if len(models) == 1:
            kws = {"model": models[0]}
        return self.config.project.get_field(field_name, view_name=view_name, **kws)

    def list_metrics(
        self,
        view_name: str = None,
        names_only: bool = False,
        show_hidden: bool = False,
    ):
        all_fields = self.config.project.fields(view_name=view_name, show_hidden=show_hidden)
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

    def list_dimensions(
        self,
        view_name: str = None,
        names_only: bool = False,
        show_hidden: bool = False,
    ):
        all_fields = self.config.project.fields(view_name=view_name, show_hidden=show_hidden)
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
        views = self.config.project.views()
        if names_only:
            return [v.name for v in views]
        return views

    def get_view(self, view_name: str):
        return self.config.project.get_view(view_name)

    def list_connections(self, names_only=False):
        connections = self.config.connections()
        if names_only:
            return [c.name for c in connections]
        return connections

    def get_connection(self, connection_name: str):
        return self.config.get_connection(connection_name)

    def list_models(self, names_only=False):
        models = self.config.project.models()
        if names_only:
            return [m.name for m in models]
        return models

    def get_model(self, model_name: str):
        return self.config.project.get_model(model_name)

    def list_dashboards(self, names_only=False):
        dashboards = self.config.project.dashboards()
        if names_only:
            return [d.name for d in dashboards]
        return dashboards

    def get_dashboard(self, dashboard_name: str):
        return self.config.project.get_dashboard(dashboard_name)

    @staticmethod
    def pretty_sql(sql: str, keyword_case="lower"):
        return sqlparse.format(sql, reindent=True, keyword_case=keyword_case)
