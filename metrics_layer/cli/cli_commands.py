import sys

from metrics_layer.core.utils import lazy_import

lazy_click = lazy_import("click")

from .seeding import SeedMetricsLayer


def echo(text: str, color: str = None, bold: bool = True):
    if color:
        lazy_click.secho(text, fg=color, bold=bold)
    else:
        lazy_click.echo(text)


@lazy_click.group()
# @lazy_click.version_option(version="0.22")
def cli_group():
    pass


@cli_group.command()
def test(arg=None, opt={}):
    """Initialize a metrics layer project"""
    echo("testing", color="red")


@cli_group.command()
def init(arg=None, opt={}):
    """Initialize a metrics layer project"""
    SeedMetricsLayer._init_directories()


@cli_group.command()
@lazy_click.option("--connection", default=None, help="The name of the connection to use for the database")
@lazy_click.option("--database", help="The name of the database to use for seeding")
@lazy_click.option("--schema", default=None, help="The name of the schema to use for seeding")
@lazy_click.argument("profile")
def seed(profile, connection, database, schema):
    """Seed a metrics layer project by referencing the existing database"""
    SeedMetricsLayer._init_directories()
    seeder = SeedMetricsLayer(profile, connection, database, schema)
    seeder.seed()


@cli_group.command()
@lazy_click.argument("profile")
def validate(profile):
    """Validate a metrics layer project, internally, without hitting the database"""
    metrics_layer = SeedMetricsLayer._init_profile(profile)
    errors = metrics_layer.config.project.validate()

    if len(errors) == 0:
        n_explores = len(metrics_layer.config.project.explores())
        echo(f"Project passed (checked {n_explores} explore{'s' if n_explores > 1 else ''})!")
    else:
        echo(f"Found {len(errors)} error{'s' if len(errors)> 1 else ''} in the project:\n")
        for error in errors:
            echo(f"\n{error}\n")


@cli_group.command()
@lazy_click.argument("profile")
def debug(profile):
    """Debug a metrics layer project. Pass your profile name as the sole argument.

    This will list out the inputs and the locations where the metrics layer is finding them,
     in addition to testing the database connection to ensure it works as expected"""
    from metrics_layer import __version__

    metrics_layer = SeedMetricsLayer._init_profile(profile)

    # Environment
    metrics_layer_version = __version__
    echo(f"metrics_layer version: {metrics_layer_version}")

    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    echo(f"python version: {python_version}")

    python_path = sys.executable
    echo(f"python path: {python_path}")

    profiles_path = metrics_layer.config.profiles_path
    if profiles_path:
        echo(f"Using profiles.yml file at {profiles_path}")
    else:
        echo(f"Could not find profiles.yml file", color="red")

    # Configuration:
    echo(f"\nConfiguration:")
    if metrics_layer.config.profiles_path:
        echo(f"  profiles.yml file OK found and valid", color="green")
    else:
        echo(f"  profiles.yml file Not found", color="red")

    echo(f"\nRequired dependencies:")

    git_status = "OK found" if SeedMetricsLayer._test_git() else "Not found"
    echo(f"  git [{git_status}]", color="green" if "OK" in git_status else "red")

    # Connection:
    connections = metrics_layer.config.connections()
    echo(f"\nConnection{'s' if len(connections) > 1 else ''}:")
    for connection in connections:
        for key, value in connection.printable_attributes().items():
            echo(f"  {key}: {value}")

    test_query = "select 1 as id;"
    for connection in connections:
        try:
            metrics_layer.run_query(test_query, connection)
            connection_status = "OK connection ok"
            color = "green"
        except Exception as e:
            color = "red"
            connection_status = f"Failed with:\n\n {e}"

        echo(f"\nConnection {connection.name} test: {connection_status}", color=color)


@cli_group.command("list")
@lazy_click.option("--profile", help="The name of the profile you are using (in profiles.yml)")
@lazy_click.option(
    "--explore",
    default=None,
    help="The name of the explore (only applicable for fields, dimensions, metrics, and views)",
)
@lazy_click.option(
    "--view", default=None, help="The name of the view (only applicable for fields, dimensions, and metrics)"
)
@lazy_click.option("--show-hidden", is_flag=True, help="Set this flag if you want to see hidden fields")
@lazy_click.argument("type")
def list_(profile, type, explore, view, show_hidden):
    """List attributes in a metrics layer project,
    i.e. models, connections, explores, views, fields, metrics, dimensions"""
    if profile:
        metrics_layer = SeedMetricsLayer._init_profile(profile)
    elif not profile and type != "profiles":
        lazy_click.echo(
            f"Could not find profile in environment, please pass the "
            "name of your profile with the --profile flag"
        )
        return

    items = None
    if type == "models":
        items = metrics_layer.list_models(names_only=True)
    elif type == "connections":
        items = metrics_layer.list_connections(names_only=True)
    elif type == "explores":
        items = metrics_layer.list_explores(names_only=True, show_hidden=show_hidden)
    elif type == "views":
        items = metrics_layer.list_views(names_only=True, explore_name=explore, show_hidden=show_hidden)
    elif type == "fields":
        items = metrics_layer.list_fields(
            names_only=True, view_name=view, explore_name=explore, show_hidden=show_hidden
        )
    elif type == "dimensions":
        items = metrics_layer.list_dimensions(
            names_only=True, view_name=view, explore_name=explore, show_hidden=show_hidden
        )
    elif type == "metrics":
        items = metrics_layer.list_metrics(
            names_only=True, view_name=view, explore_name=explore, show_hidden=show_hidden
        )
    elif type == "profiles":
        if profile:
            items = metrics_layer.config.get_all_profiles(names_only=True)
        else:
            from metrics_layer.core.parse import MetricsLayerConfiguration

            default_directory = MetricsLayerConfiguration.get_metrics_layer_directory() + "profiles.yml"
            items = MetricsLayerConfiguration.get_all_profiles(default_directory, names_only=True)
    else:
        lazy_click.echo(
            f"Could not find the type {type}, please use one of the options: "
            "models, connections, explores, views, fields, metrics, dimensions"
        )

    if items:
        lazy_click.echo(f"Found {len(items)} {type if len(items) > 1 else type[:-1]}:\n")
        for name in items:
            lazy_click.echo(name)

    if items is not None and len(items) == 0:
        lazy_click.echo(f"Could not find any {type}")


@cli_group.command()
@lazy_click.option("--profile", help="The name of the profile you are using (in profiles.yml)")
@lazy_click.option(
    "--type",
    help="The type of object to show. One of: model, connection, explore, view, field, metric, dimension",  # noqa
)
@lazy_click.option(
    "--explore",
    default=None,
    help="The name of the explore (only applicable for fields, dimensions, metrics, and views)",
)
@lazy_click.option(
    "--view", default=None, help="The name of the view (only applicable for fields, dimensions, and metrics)"
)
@lazy_click.argument("name")
def show(profile, type, name, explore, view):
    """Show information on an attribute in a metrics layer project, by name"""
    metrics_layer = SeedMetricsLayer._init_profile(profile)

    attributes = []
    if type == "model":
        attributes = metrics_layer.get_model(name)
    elif type == "connection":
        attributes = metrics_layer.get_connection(name)
    elif type == "explore":
        attributes = metrics_layer.get_explore(name)
    elif type == "view":
        attributes = metrics_layer.get_view(name, explore_name=explore)
    elif type == "field":
        attributes = metrics_layer.get_field(name, view_name=view, explore_name=explore)
    elif type == "dimension":
        attributes = metrics_layer.get_dimension(name, view_name=view, explore_name=explore)
    elif type == "metric":
        attributes = metrics_layer.get_metric(name, view_name=view, explore_name=explore)
    else:
        lazy_click.echo(
            f"Could not find the type {type}, please use one of the options: "
            "models, connections, explores, views, fields, metrics, dimensions"
        )

    if attributes:
        lazy_click.echo(f"Attributes in {type} {name}:\n")
        for key, value in attributes.printable_attributes().items():
            if isinstance(value, list) and len(value) > 0:
                lazy_click.echo(f"  {key}:")
                for item in value:
                    lazy_click.echo(f"    {item}")
            else:
                lazy_click.echo(f"  {key}: {value}")


if __name__ == "__main__":
    cli_group()
