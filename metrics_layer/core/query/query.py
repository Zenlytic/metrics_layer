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
                where=where,
                having=having,
                order_by=order_by,
                config=self.config,
                **{**self.kwargs, **kwargs},
            )
            query = resolver.get_query()
            connection = resolver.connection

        if kwargs.get("return_connection", False):
            return query, connection
        return query

    def run_query(self, query: str, connection: BaseConnection, **kwargs):
        runner = QueryRunner(query, connection)
        df = runner.run_query(**{**self.kwargs, **kwargs})
        return df

    def define(self, metric: str, explore_name: str = None, query_type: str = None):
        field = self.config.project.get_field(metric, explore_name=explore_name)
        return field.sql_query(query_type)

    def list_fields(
        self,
        explore_name: str = None,
        view_name: str = None,
        names_only: bool = False,
        show_hidden: bool = False,
    ):
        all_fields = self.config.project.fields(
            explore_name=explore_name, view_name=view_name, show_hidden=show_hidden
        )
        if names_only:
            return [m.name for m in all_fields]
        return all_fields

    def get_field(self, field_name: str, explore_name: str = None, view_name: str = None):
        return self.config.project.get_field(field_name, explore_name=explore_name, view_name=view_name)

    def list_metrics(
        self,
        explore_name: str = None,
        view_name: str = None,
        names_only: bool = False,
        show_hidden: bool = False,
    ):
        all_fields = self.config.project.fields(
            explore_name=explore_name, view_name=view_name, show_hidden=show_hidden
        )
        metrics = [f for f in all_fields if f.field_type == "measure"]
        if names_only:
            return [m.name for m in metrics]
        return metrics

    def get_metric(self, metric_name: str, explore_name: str = None, view_name: str = None):
        metrics = self.list_metrics(explore_name=explore_name, view_name=view_name)
        try:
            metric = next((m for m in metrics if m.equal(metric_name)))
            return metric
        except ParseError as e:
            raise e(f"Could not find metric {metric_name} in the project config")

    def list_dimensions(
        self,
        explore_name: str = None,
        view_name: str = None,
        names_only: bool = False,
        show_hidden: bool = False,
    ):
        all_fields = self.config.project.fields(
            explore_name=explore_name, view_name=view_name, show_hidden=show_hidden
        )
        dimensions = [f for f in all_fields if f.field_type in {"dimension", "dimension_group"}]
        if names_only:
            return [d.name for d in dimensions]
        return dimensions

    def get_dimension(self, dimension_name: str, explore_name: str = None, view_name: str = None):
        dimensions = self.list_dimensions(explore_name=explore_name, view_name=view_name)
        try:
            dimension = next((d for d in dimensions if d.equal(dimension_name)))
            return dimension
        except ParseError as e:
            raise e(f"Could not find dimension {dimension_name} in the project config")

    def list_views(self, names_only=False, explore_name: str = None, show_hidden: bool = False):
        views = self.config.project.views(explore_name=explore_name)
        if names_only:
            return [v.name for v in views]
        return views

    def get_view(self, view_name: str, explore_name: str = None):
        if explore_name:
            explore = self.get_explore(explore_name)
        else:
            explore = None
        return self.config.project.get_view(view_name, explore=explore)

    def list_explores(self, names_only=False, show_hidden: bool = False):
        explores = self.config.project.explores(show_hidden=show_hidden)
        if names_only:
            return [e.name for e in explores]
        return explores

    def get_explore(self, explore_name: str):
        return self.config.project.get_explore(explore_name=explore_name)

    def list_connections(self, names_only=False, show_hidden: bool = False):
        connections = self.config.connections()
        if names_only:
            return [c.name for c in connections]
        return connections

    def get_connection(self, connection_name: str):
        return self.config.get_connection(connection_name)

    def list_models(self, names_only=False, show_hidden: bool = False):
        models = self.config.project.models()
        if names_only:
            return [m.name for m in models]
        return models

    def get_model(self, model_name: str):
        return self.config.project.get_model(model_name)

    def list_dashboards(self, names_only=False, show_hidden: bool = False):
        dashboards = self.config.project.dashboards()
        if names_only:
            return [d.name for d in dashboards]
        return dashboards

    def get_dashboard(self, dashboard_name: str):
        return self.config.project.get_dashboard(dashboard_name)
