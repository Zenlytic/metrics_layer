import click

from .seeding import SeedMetricsLayer


@click.group()
def cli():
    pass


@cli.command()
def init(arg=None, opt={}):
    """Initialize a metrics layer project"""
    SeedMetricsLayer._init_directories()


@cli.command()
@click.option("--profile", help="The profile to use in accessing your database")
@click.option("--connection", default=None, help="The name of the connection to use for the database")
@click.option("--database", help="The name of the database to use for seeding")
@click.option("--schema", default=None, help="The name of the schema to use for seeding")
def seed(profile, connection, database, schema):
    """Seed a metrics layer project by referencing the existing database"""
    SeedMetricsLayer._init_directories()
    seeder = SeedMetricsLayer(profile, connection, database, schema)
    seeder.seed()


@cli.command()
@click.option("--opt")
@click.argument("arg")
# @click.option('--count', default=1, help='Number of greetings.')
# @click.option('--name', prompt='Your name',
def validate(arg, opt):
    """Validate a metrics layer project, internally, without hitting the database"""
    click.echo("Opt: {}  Arg: {}".format(opt, arg))


if __name__ == "__main__":
    cli()
