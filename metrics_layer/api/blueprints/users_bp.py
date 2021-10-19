from flask import Blueprint, request
from flask_restful import Api, Resource
from sqlalchemy import exc

from metrics_layer.api import bcrypt, db
from metrics_layer.api.auth_utils import authenticate_restful
from metrics_layer.api.models import User

users_blueprint = Blueprint("users", __name__, url_prefix="/api/v1")
api = Api(users_blueprint)


class Users(Resource):
    method_decorators = {
        "get": [authenticate_restful],
        "patch": [authenticate_restful],
        "delete": [authenticate_restful],
    }

    def get(self, user_id: str):
        """Get single user details"""
        response = {"status": "failure", "message": "User does not exist"}
        try:
            user = User.query.filter_by(id=int(user_id)).first()
            if not user:
                return response, 404
            return {"status": "success", "data": user.to_json()}, 200

        except ValueError:
            return response, 404

    def patch(self, user_id: str):
        request_json = request.get_json()
        user = User.modify(user_id, request_json)
        return {"status": "success", "data": user.to_json()}, 200

    def delete(self, user_id: str):
        user = User.delete(user_id)
        if not user:
            return {"status": "failure", "message": "User does not exist"}, 404
        return {"status": "success"}


class UsersList(Resource):
    method_decorators = {"get": [authenticate_restful]}

    def get(self):
        """Get all users"""
        response_object = {
            "status": "success",
            "data": {"users": [user.to_json() for user in User.query.all()]},
        }
        return response_object, 200


class Register(Resource):
    def post(self):
        post_data = request.get_json()
        failure_response = {"status": "failure", "message": "Invalid payload."}
        if not post_data:
            return failure_response, 400

        email = post_data.get("email")
        password = post_data.get("password")  # new

        try:
            user = User.query.filter_by(email=email).first()
            if not user:
                new_user = User.create(email=email, password=password)
                auth_token = new_user.encode_auth_token(new_user.id)
                return {"status": "success", "message": f"{email} was added", "auth_token": auth_token}, 200
            else:
                failure_response["message"] = "Sorry. That email already exists."
                return failure_response, 400

        except (exc.IntegrityError, ValueError):
            db.session.rollback()
            return failure_response, 400


class Login(Resource):
    def post(self):
        post_data = request.get_json()
        failure_response = {"status": "failure", "message": "Invalid payload."}
        if not post_data:
            return failure_response, 400

        email = post_data.get("email")
        password = post_data.get("password")

        try:
            # fetch the user data
            user = User.query.filter_by(email=email).first()

            if user and bcrypt.check_password_hash(user.password, password):
                auth_token = user.encode_auth_token(user.id)
                if auth_token:
                    response = {
                        "status": "success",
                        "message": "Successfully logged in.",
                        "auth_token": auth_token,
                    }
                    return response, 200

            else:
                failure_response["message"] = "User does not exist."
                return failure_response, 404

        except Exception:
            failure_response["message"] = "Try again."
            return failure_response, 500


class Logout(Resource):
    method_decorators = {"get": [authenticate_restful]}

    def get(self, sub):
        response = {"status": "success", "message": "Successfully logged out."}
        return response, 200


api.add_resource(Register, "/register")
api.add_resource(Login, "/login")
api.add_resource(Logout, "/logout")
api.add_resource(UsersList, "/users")
api.add_resource(Users, "/users/<user_id>")
