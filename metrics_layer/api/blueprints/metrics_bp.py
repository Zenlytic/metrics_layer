from flask import Blueprint
from flask_restful import Api, Resource

from metrics_layer.api.auth_utils import authenticate_restful
from metrics_layer.core import MetricsLayerConnection
from metrics_layer.core.sql.query_errors import ParseError

metrics_blueprint = Blueprint("metrics", __name__, url_prefix="/api/v1")
api = Api(metrics_blueprint)


class Metrics(Resource):
    method_decorators = {"get": [authenticate_restful]}

    def get(self, sub, metric_name: str):
        try:
            conn = MetricsLayerConnection()
            metric = conn.get_metric(metric_name)
        except ParseError:
            return {"status": "failure", "message": f"metric {metric_name} not found"}, 404

        return {"status": "success", "data": metric.to_dict()}, 200


class MetricsList(Resource):
    method_decorators = {"get": [authenticate_restful]}

    def get(self, sub):
        conn = MetricsLayerConnection()
        metrics = conn.list_metrics()
        metrics_json = [m.to_dict() for m in metrics]
        return {"status": "success", "data": metrics_json}, 200


class Dimensions(Resource):
    method_decorators = {"get": [authenticate_restful]}

    def get(self, sub, dimension_name: str):
        try:
            conn = MetricsLayerConnection()
            dimension = conn.get_dimension(dimension_name)
        except ParseError:
            return {"status": "failure", "message": f"dimension {dimension_name} not found"}, 404

        return {"status": "success", "data": dimension.to_dict()}, 200


class DimensionsList(Resource):
    method_decorators = {"get": [authenticate_restful]}

    def get(self, sub):
        conn = MetricsLayerConnection()
        dimensions = conn.list_dimensions()
        dimensions_json = [d.to_dict() for d in dimensions]
        return {"status": "success", "data": dimensions_json}, 200


api.add_resource(MetricsList, "/metrics")
api.add_resource(Metrics, "/metrics/<metric_name>")
api.add_resource(DimensionsList, "/dimensions")
api.add_resource(Dimensions, "/dimensions/<dimension_name>")
