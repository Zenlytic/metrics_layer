import os

BASE_PATH = os.path.dirname(__file__)


class BaseConfig:
    """Base configuration"""

    TESTING = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = os.getenv("SECRET_KEY")
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL")
    BCRYPT_LOG_ROUNDS = 13
    TOKEN_EXPIRATION_DAYS = 30
    TOKEN_EXPIRATION_SECONDS = 0


class TestingConfig(BaseConfig):
    """Testing configuration"""

    TESTING = True
    PRESERVE_CONTEXT_ON_EXCEPTION = False
    SECRET_KEY = "test-key"
    _default_testing_uri = "sqlite:///" + os.path.join(BASE_PATH, "test.db")
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_TEST_URL", _default_testing_uri)
    BCRYPT_LOG_ROUNDS = 4
    TOKEN_EXPIRATION_DAYS = 0
    TOKEN_EXPIRATION_SECONDS = 3
