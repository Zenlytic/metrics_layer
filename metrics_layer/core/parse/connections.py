import json
import os
from copy import deepcopy

from metrics_layer.core.model.definitions import Definitions


class MetricsLayerConnectionError(Exception):
    pass


class ConnectionType:
    snowflake = Definitions.snowflake
    bigquery = Definitions.bigquery


class BaseConnection:
    def __repr__(self):
        return f"<{self.__class__.__name__} name={self.name}>"


class SnowflakeConnection(BaseConnection):
    def __init__(
        self,
        name: str,
        account: str,
        username: str,
        password: str,
        role: str = None,
        warehouse: str = None,
        database: str = None,
        schema: str = None,
        **kwargs,
    ) -> None:
        self.type = ConnectionType.snowflake
        self.name = name
        self.account = account
        self.username = username
        self.password = password
        self.role = role
        self.warehouse = warehouse
        self.database = database
        self.schema = schema

    def to_dict(self):
        """Dict for use with the snowflake connector"""
        base = {"user": self.username, "password": self.password, "account": self.account}
        if self.warehouse:
            base["warehouse"] = self.warehouse
        if self.database:
            base["database"] = self.database
        if self.schema:
            base["schema"] = self.schema
        if self.role:
            base["role"] = self.role
        return base

    def printable_attributes(self):
        attributes = deepcopy(self.to_dict())
        attributes.pop("password")
        attributes["name"] = self.name
        sort_order = ["name", "account", "user", "database", "schema", "warehouse", "role"]
        return {key: attributes.get(key) for key in sort_order if attributes.get(key) is not None}


class BigQueryConnection(BaseConnection):
    def __init__(self, name: str, credentials: str, **kwargs) -> None:
        self.type = ConnectionType.bigquery
        self.name = name
        self.credentials = self._convert_json_if_needed(credentials, kwargs)
        self.project_id = self.credentials["project_id"]

    def to_dict(self):
        """Dict for use with the BigQuery connector"""
        return {"credentials": self.credentials, "project_id": self.project_id}

    def printable_attributes(self):
        attributes = deepcopy(self.to_dict())
        attributes.pop("credentials")
        attributes["name"] = self.name
        sort_order = ["name", "project_id"]
        return {key: attributes.get(key) for key in sort_order if attributes.get(key) is not None}

    @staticmethod
    def _convert_json_if_needed(creds: dict, kwargs: dict):
        if isinstance(creds, dict):
            return deepcopy(creds)
        elif isinstance(creds, str):
            try:
                return json.loads(creds)
            except json.JSONDecodeError:
                # This means it's a file path not a JSON string
                if os.path.isabs(creds):
                    path = creds
                else:
                    path = os.path.join(kwargs["directory"], creds)

                with open(path, "r") as f:
                    return json.load(f)
        else:
            raise TypeError(f"BigQuery credentials json had wrong type: {type(creds)} for value {creds}")
