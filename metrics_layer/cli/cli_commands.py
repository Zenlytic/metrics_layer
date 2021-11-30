from metrics_layer.core.utils import lazy_import

lazy_click = lazy_import("click")

from .seeding import SeedMetricsLayer


@lazy_click.group()
def cli_group():
    pass


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
        lazy_click.echo(f"Project passed (checked {n_explores} explore{'s' if n_explores > 1 else ''})!")
    else:
        lazy_click.echo(f"Found {len(errors)} error{'s' if len(errors)> 1 else ''} in the project:\n")
        for error in errors:
            lazy_click.echo(f"\n{error}\n")


if __name__ == "__main__":
    cli_group()
