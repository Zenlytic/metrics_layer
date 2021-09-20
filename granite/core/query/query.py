from granite.core.model.project import Project
from granite.core.sql.resolve import SQLQueryResolver


def query(
    metrics: list,
    dimensions: list = [],
    where: list = [],
    having: list = [],
    order_by: list = [],
    project: Project = None,
    **kwargs,
):
    resolver = SQLQueryResolver(
        metrics=metrics,
        dimensions=dimensions,
        where=where,
        having=having,
        order_by=order_by,
        project=project,
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
    project: Project = None,
    **kwargs,
):
    resolver = SQLQueryResolver(
        metrics=metrics,
        dimensions=dimensions,
        where=where,
        having=having,
        order_by=order_by,
        project=project,
        **kwargs,
    )
    return resolver.get_query()


def define(metric: str, project: Project = None):
    field = project.get_field(metric)
    return field.sql_query()


def list_metrics(
    explore_name: str = None, view_name: str = None, names_only: bool = False, project: Project = None
):
    all_fields = project.fields(explore_name=explore_name, view_name=view_name)
    metrics = [f for f in all_fields if f.field_type == "measure"]
    if names_only:
        return [m.name for m in metrics]
    return metrics


def list_dimensions(
    explore_name: str = None, view_name: str = None, names_only: bool = False, project: Project = None
):
    all_fields = project.fields(explore_name=explore_name, view_name=view_name)
    dimensions = [f for f in all_fields if f.field_type in {"dimension", "dimension_group"}]
    if names_only:
        return [d.name for d in dimensions]
    return dimensions
