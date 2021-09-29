from flask import Blueprint
from flask_restful import Api, Resource

from granite.api.auth_utils import authenticate_restful
from granite.core import query as granite_query
from granite.core.sql.query_errors import ParseError

metrics_blueprint = Blueprint("metrics", __name__, url_prefix="/api/v1")
api = Api(metrics_blueprint)


class Metrics(Resource):
    method_decorators = {"get": [authenticate_restful]}

    def get(self, sub, metric_name: str):
        try:
            metric = granite_query.get_metric(metric_name)
        except ParseError:
            return {"status": "failure", "message": f"metric {metric_name} not found"}, 404

        return {"status": "success", "data": metric.to_dict()}, 200


class MetricsList(Resource):
    method_decorators = {"get": [authenticate_restful]}

    def get(self, sub):
        metrics = granite_query.list_metrics()
        metrics_json = [m.to_dict() for m in metrics]
        return {"status": "success", "data": metrics_json}, 200


class Dimensions(Resource):
    method_decorators = {"get": [authenticate_restful]}

    def get(self, sub, dimension_name: str):
        try:
            dimension = granite_query.get_dimension(dimension_name)
        except ParseError:
            return {"status": "failure", "message": f"dimension {dimension_name} not found"}, 404

        return {"status": "success", "data": dimension.to_dict()}, 200


class DimensionsList(Resource):
    method_decorators = {"get": [authenticate_restful]}

    def get(self, sub):
        dimensions = granite_query.list_dimensions()
        dimensions_json = [d.to_dict() for d in dimensions]
        return {"status": "success", "data": dimensions_json}, 200


api.add_resource(MetricsList, "/metrics")
api.add_resource(Metrics, "/metrics/<metric_name>")
api.add_resource(DimensionsList, "/dimensions")
api.add_resource(Dimensions, "/dimensions/<dimension_name>")
