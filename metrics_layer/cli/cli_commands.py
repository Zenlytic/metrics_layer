import sys

import click

from .seeding import SeedMetricsLayer, dbtSeed


def echo(text: str, color: str = None, bold: bool = True):
    if color:
        click.secho(text, fg=color, bold=bold)
    else:
        click.echo(text)


@click.group()
@click.version_option()
def cli_group():
    pass


@cli_group.command()
def init():
    """Initialize a metrics layer project"""
    SeedMetricsLayer._init_directories()
    SeedMetricsLayer._init_project_file()


@cli_group.command()
@click.option("--connection", default=None, help="The name of the connection to use for the database")
@click.option("--database", help="The name of the database to use for seeding (project in BigQuery)")
@click.option(
    "--schema", default=None, help="The name of the schema to use for seeding (dataset in BigQuery)"
)
@click.option("--table", default=None, help="The name of the table to use for seeding")
def seed(connection, database, schema, table):
    """Seed a metrics layer project by referencing the existing database"""
    SeedMetricsLayer._init_directories()
    profile = SeedMetricsLayer.get_profile()
    if SeedMetricsLayer._in_dbt_project():
        seeder = dbtSeed(profile, connection, database, schema, table)
    else:
        seeder = SeedMetricsLayer(profile, connection, database, schema, table)
    seeder.seed()


@cli_group.command()
def validate():
    """Validate a metrics layer project, internally, without hitting the database"""
    profile = SeedMetricsLayer.get_profile()
    metrics_layer = SeedMetricsLayer._init_profile(profile)
    errors = metrics_layer.project.validate()

    if len(errors) == 0:
        n_models = len(metrics_layer.project.models())
        echo(f"Project passed (checked {n_models} model{'s' if n_models > 1 else ''})!")
    else:
        echo(f"Found {len(errors)} error{'s' if len(errors)> 1 else ''} in the project:\n")
        for error in errors:
            echo(f"\n{error}\n")


@cli_group.command()
def debug():
    """Debug a metrics layer project. Pass your profile name as the sole argument.

    This will list out the inputs and the locations where the metrics layer is finding them,
     in addition to testing the database connection to ensure it works as expected"""
    from metrics_layer import __version__

    profile = SeedMetricsLayer.get_profile()
    metrics_layer = SeedMetricsLayer._init_profile(profile)

    # Environment
    metrics_layer_version = __version__
    echo(f"metrics_layer version: {metrics_layer_version}")

    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    echo(f"python version: {python_version}")

    python_path = sys.executable
    echo(f"python path: {python_path}")

    profiles_path = metrics_layer.profiles_path
    if profiles_path:
        echo(f"Using profiles.yml file at {profiles_path}")
    else:
        echo(f"Could not find profiles.yml file", color="red")

    # Configuration:
    echo(f"\nConfiguration:")
    if metrics_layer.profiles_path:
        echo(f"  profiles.yml file OK found and valid", color="green")
    else:
        echo(f"  profiles.yml file Not found", color="red")

    echo(f"\nRequired dependencies:")

    git_status = "OK found" if SeedMetricsLayer._test_git() else "Not found"
    echo(f"  git [{git_status}]", color="green" if "OK" in git_status else "red")

    # Connection:
    connections = metrics_layer.list_connections()
    echo(f"\nConnection{'s' if len(connections) > 1 else ''}:")
    for connection in connections:
        for key, value in connection.printable_attributes().items():
            echo(f"  {key}: {value}")

    test_query = "select 1 as id;"
    for connection in connections:
        try:
            metrics_layer.run_query(test_query, connection, run_pre_queries=False)
            connection_status = "OK connection ok"
            color = "green"
        except Exception as e:
            color = "red"
            connection_status = f"Failed with:\n\n {e}"

        echo(f"\nConnection {connection.name} test: {connection_status}", color=color)


@cli_group.command("list")
@click.option(
    "--view", default=None, help="The name of the view (only applicable for fields, dimensions, and metrics)"
)
@click.option("--show-hidden", is_flag=True, help="Set this flag if you want to see hidden fields")
@click.argument("type")
def list_(type, view, show_hidden):
    """List attributes in a metrics layer project,
    i.e. models, connections, explores, views, fields, metrics, dimensions"""
    profile = SeedMetricsLayer.get_profile()
    metrics_layer = SeedMetricsLayer._init_profile(profile)

    items = None
    if type == "models":
        items = metrics_layer.list_models(names_only=True)
    elif type == "connections":
        items = metrics_layer.list_connections(names_only=True)
    elif type == "views":
        items = metrics_layer.list_views(names_only=True)
    elif type == "fields":
        items = metrics_layer.list_fields(names_only=True, view_name=view, show_hidden=show_hidden)
    elif type == "dimensions":
        items = metrics_layer.list_dimensions(names_only=True, view_name=view, show_hidden=show_hidden)
    elif type == "metrics":
        items = metrics_layer.list_metrics(names_only=True, view_name=view, show_hidden=show_hidden)
    elif type == "profiles":
        items = metrics_layer.get_all_profiles(names_only=True)
    else:
        click.echo(
            f"Could not find the type {type}, please use one of the options: "
            "models, connections, views, fields, metrics, dimensions"
        )
    if items:
        click.echo(f"Found {len(items)} {type if len(items) > 1 else type[:-1]}:\n")
        for name in items:
            click.echo(name)

    if items is not None and len(items) == 0:
        click.echo(f"Could not find any {type}")


@cli_group.command()
@click.option(
    "--type",
    help="The type of object to show. One of: model, connection, explore, view, field, metric, dimension",  # noqa
)
@click.option(
    "--view", default=None, help="The name of the view (only applicable for fields, dimensions, and metrics)"
)
@click.argument("name")
def show(type, name, view):
    """Show information on an attribute in a metrics layer project, by name"""
    profile = SeedMetricsLayer.get_profile()
    metrics_layer = SeedMetricsLayer._init_profile(profile)

    attributes = []
    if type == "model":
        attributes = metrics_layer.get_model(name)
    elif type == "connection":
        attributes = metrics_layer.get_connection(name)
    elif type == "view":
        attributes = metrics_layer.get_view(name)
    elif type == "field":
        attributes = metrics_layer.get_field(name, view_name=view)
    elif type == "dimension":
        attributes = metrics_layer.get_dimension(name, view_name=view)
    elif type == "metric":
        attributes = metrics_layer.get_metric(name, view_name=view)
    else:
        click.echo(
            f"Could not find the type {type}, please use one of the options: "
            "models, connections, views, fields, metrics, dimensions"
        )

    if attributes:
        click.echo(f"Attributes in {type} {name}:\n")
        for key, value in attributes.printable_attributes().items():
            if isinstance(value, list) and len(value) > 0:
                click.echo(f"  {key}:")
                for item in value:
                    click.echo(f"    {item}")
            else:
                click.echo(f"  {key}: {value}")


if __name__ == "__main__":
    cli_group()
