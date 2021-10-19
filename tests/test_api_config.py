import os

from flask import current_app


def test_base_config(test_app):
    test_app.config.from_object("metrics_layer.api.api_config.BaseConfig")
    assert not test_app.config["TESTING"]
    assert test_app.config["SQLALCHEMY_DATABASE_URI"] == os.environ.get("DATABASE_URL")
    assert current_app is not None


def test_test_config(test_app):
    test_app.config.from_object("metrics_layer.api.api_config.TestingConfig")
    assert test_app.config["TESTING"]
    assert current_app is not None
    assert not test_app.config["PRESERVE_CONTEXT_ON_EXCEPTION"]
