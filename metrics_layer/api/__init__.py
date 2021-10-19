import logging

from flask import Flask, jsonify
from flask_bcrypt import Bcrypt
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy

from metrics_layer.api.logging_utils import FORMAT, setup_logging

setup_logging()

logger = logging.getLogger("metrics_layer.api")

db = SQLAlchemy()
migrate = Migrate()
bcrypt = Bcrypt()


def create_app(config="metrics_layer.api.api_config.BaseConfig"):

    # Instantiate the app
    app = Flask(__name__)

    # Setup logging
    log_handler = logging.StreamHandler()
    formatter = logging.Formatter(fmt=FORMAT)
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)

    # Set config
    app.config.from_object(config)

    # Set extensions
    db.init_app(app)
    migrate.init_app(app, db)
    bcrypt.init_app(app)

    # Register blueprints
    import metrics_layer.api.blueprints as bp

    app.register_blueprint(bp.health_blueprint)
    app.register_blueprint(bp.sql_blueprint)
    app.register_blueprint(bp.users_blueprint)
    app.register_blueprint(bp.metrics_blueprint)
    app.register_blueprint(bp.root_blueprint)

    @app.errorhandler(500)
    def internal_error(exception):
        logger.info(f"Error: {exception}")
        return jsonify({"error": True, "code": str(exception)}), 500

    # shell context for flask cli
    @app.shell_context_processor
    def ctx():
        return {"app": app, "db": db}

    return app
