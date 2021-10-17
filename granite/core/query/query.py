from typing import Union

from granite.core.convert import MQLConverter
from granite.core.parse import GraniteConfiguration
from granite.core.parse.connections import BaseConnection
from granite.core.sql import QueryRunner, SQLQueryResolver
from granite.core.sql.query_errors import ParseError


class GraniteConnection:
    def __init__(self, config: Union[GraniteConfiguration, str, dict] = None, target: str = None, **kwargs):
        if isinstance(config, str):
            self.config = GraniteConfiguration(config, target=target)
        if isinstance(config, dict):
            self.config = GraniteConfiguration(
                config, connections=config.get("connections", []), target=target
            )
        else:
            self.config = GraniteConfiguration.get_granite_configuration(config, target=target)
        self.kwargs = kwargs

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

    def define(self, metric: str):
        field = self.config.project.get_field(metric)
        return field.sql_query()

    def list_metrics(self, explore_name: str = None, view_name: str = None, names_only: bool = False):
        all_fields = self.config.project.fields(explore_name=explore_name, view_name=view_name)
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

    def list_dimensions(self, explore_name: str = None, view_name: str = None, names_only: bool = False):
        all_fields = self.config.project.fields(explore_name=explore_name, view_name=view_name)
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

    def list_explores(self, names_only=False):
        explores = self.config.project.explores()
        if names_only:
            return [e.name for e in explores]
        return explores

    def get_explore(self, explore_name: str):
        return self.config.project.get_explore(explore_name=explore_name)
