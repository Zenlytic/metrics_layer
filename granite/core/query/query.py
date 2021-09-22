from granite.core.parse import GraniteConfiguration
from granite.core.sql import SQLQueryResolver


def query(
    metrics: list,
    dimensions: list = [],
    where: list = [],
    having: list = [],
    order_by: list = [],
    config: GraniteConfiguration = None,
    **kwargs,
):
    working_config = get_granite_configuration(config)
    resolver = SQLQueryResolver(
        metrics=metrics,
        dimensions=dimensions,
        where=where,
        having=having,
        order_by=order_by,
        project=working_config.project,
        **kwargs,
    )
    resolver.get_query()
    #
    raise NotImplementedError()


def get_sql_query(
    metrics: list,
    dimensions: list = [],
    where: list = [],
    having: list = [],
    order_by: list = [],
    config: GraniteConfiguration = None,
    **kwargs,
):
    working_config = get_granite_configuration(config)
    resolver = SQLQueryResolver(
        metrics=metrics,
        dimensions=dimensions,
        where=where,
        having=having,
        order_by=order_by,
        project=working_config.project,
        **kwargs,
    )
    return resolver.get_query()


def define(
    metric: str,
    config: GraniteConfiguration = None,
):
    working_config = get_granite_configuration(config)
    field = working_config.project.get_field(metric)
    return field.sql_query()


def list_metrics(
    explore_name: str = None,
    view_name: str = None,
    names_only: bool = False,
    config: GraniteConfiguration = None,
):
    working_config = get_granite_configuration(config)
    all_fields = working_config.project.fields(explore_name=explore_name, view_name=view_name)
    metrics = [f for f in all_fields if f.field_type == "measure"]
    if names_only:
        return [m.name for m in metrics]
    return metrics


def list_dimensions(
    explore_name: str = None,
    view_name: str = None,
    names_only: bool = False,
    config: GraniteConfiguration = None,
):
    working_config = get_granite_configuration(config)
    all_fields = working_config.project.fields(explore_name=explore_name, view_name=view_name)
    dimensions = [f for f in all_fields if f.field_type in {"dimension", "dimension_group"}]
    if names_only:
        return [d.name for d in dimensions]
    return dimensions


def get_granite_configuration(config: GraniteConfiguration):
    if config:
        return config
    return GraniteConfiguration()
