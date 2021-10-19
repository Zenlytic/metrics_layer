from flask.cli import FlaskGroup

from metrics_layer.api import create_app, db
from metrics_layer.api.models import User

cli = FlaskGroup(create_app=create_app)


@cli.command("recreate_db")
def recreate_db():
    db.drop_all()
    db.create_all()
    db.session.commit()


@cli.command("seed_db")
def seed_db():
    """Seeds the database."""
    User.create(
        first_name="First",
        last_name="Last",
        email="test@example.com",
        password="password",
    )


if __name__ == "__main__":
    cli()
