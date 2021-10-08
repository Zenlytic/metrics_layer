from flask import Blueprint, request
from flask_restful import Api, Resource

from granite.api.auth_utils import authenticate_restful
from granite.core.query import get_sql_query, query

sql_blueprint = Blueprint("sql", __name__, url_prefix="/api/v1")
api = Api(sql_blueprint)


class ConvertApi(Resource):
    method_decorators = {"post": [authenticate_restful]}

    def post(self, sub):
        request_json = request.get_json()

        if "query" not in request_json:
            return {"status": "failure", "message": "Request missing required parameter 'query'"}, 400

        converted_query = get_sql_query(sql=request_json["query"])
        return {"status": "success", "data": converted_query}


class QueryApi(Resource):
    method_decorators = {"post": [authenticate_restful]}

    def post(self, sub):
        request_json = request.get_json()

        df = query(
            sql=request_json.get("sql"),
            metrics=request_json.get("metrics", []),
            dimensions=request_json.get("dimensions", []),
            where=request_json.get("where", []),
            having=request_json.get("having", []),
            order_by=request_json.get("order_by", []),
        )
        return {"status": "success", "data": df.to_dict("records")}


api.add_resource(ConvertApi, "/convert")
api.add_resource(QueryApi, "/query")