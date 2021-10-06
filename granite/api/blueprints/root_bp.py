from flask import Blueprint
from flask_restful import Api, Resource

root_blueprint = Blueprint("root", __name__)
api = Api(root_blueprint)


class Root(Resource):
    def get(self):
        return {"status": "success", "message": "TODO react app"}


api.add_resource(Root, "/")
