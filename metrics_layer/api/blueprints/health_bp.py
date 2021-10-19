from flask import Blueprint
from flask_restful import Api, Resource

health_blueprint = Blueprint("health", __name__, url_prefix="/api/v1")
api = Api(health_blueprint)


class HealthPing(Resource):
    def get(self):
        return {"status": "success", "message": "MetricsLayer server active!"}


api.add_resource(HealthPing, "/health")
