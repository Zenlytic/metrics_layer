from functools import wraps

from flask import request

from metrics_layer.api.models import User


def authenticate_restful(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        failure_response = {"status": "failure", "message": "Provide a valid auth token."}

        auth_header = request.headers.get("Authorization")
        if not auth_header:
            return failure_response, 403

        auth_token = auth_header.split(" ")[1]
        sub = User.decode_auth_token(auth_token)

        if isinstance(sub, str):
            failure_response["message"] = sub
            return failure_response, 401

        user = User.query.filter_by(id=sub).first()

        if not user or not user.active:
            return failure_response, 401
        return f(sub, *args, **kwargs)

    return decorated_function
