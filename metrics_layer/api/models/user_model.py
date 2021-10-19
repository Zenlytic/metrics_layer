import datetime

import jwt
from flask import current_app

from metrics_layer.api import bcrypt, db
from metrics_layer.api.models.base_model import CoreMixin


class User(db.Model, CoreMixin):
    id = db.Column(db.Integer, unique=True, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False)
    first_name = db.Column(db.String(255))
    last_name = db.Column(db.String(255))
    password = db.Column(db.String(255))
    active = db.Column(db.Boolean())

    def __repr__(self):
        return f"<User email={self.email}>"

    def __init__(self, email, password, first_name=None, last_name=None, active=True):
        self.email = email
        self.password = bcrypt.generate_password_hash(password).decode()
        self.first_name = first_name
        self.last_name = last_name
        self.active = active

    def encode_auth_token(self, user_id):
        """Generates the auth token"""
        try:
            payload = {
                "exp": datetime.datetime.utcnow()
                + datetime.timedelta(
                    days=current_app.config.get("TOKEN_EXPIRATION_DAYS"),
                    seconds=current_app.config.get("TOKEN_EXPIRATION_SECONDS"),
                ),
                "iat": datetime.datetime.utcnow(),
                "sub": user_id,
            }
            return jwt.encode(payload, current_app.config.get("SECRET_KEY"), algorithm="HS256")
        except Exception as e:
            return e

    @staticmethod
    def decode_auth_token(auth_token):
        """
        Decodes the auth token - :param auth_token: - :return: integer|string
        """
        try:
            payload = jwt.decode(auth_token, current_app.config.get("SECRET_KEY"), algorithms=["HS256"])
            return payload["sub"]
        except jwt.ExpiredSignatureError:
            return "Signature expired. Please log in again."
        except jwt.InvalidTokenError:
            return "Invalid token. Please log in again."

    def to_json(self):
        return {
            "id": self.id,
            "email": self.email,
            "active": self.active,
            "first_name": self.first_name,
            "last_name": self.last_name,
        }
