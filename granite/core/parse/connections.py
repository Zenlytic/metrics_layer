import json
from copy import deepcopy


class GraniteConnectionError(Exception):
    pass


class ConnectionType:
    snowflake = "SNOWFLAKE"
    bigquery = "BIGQUERY"


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


class BigQueryConnection(BaseConnection):
    def __init__(self, name: str, credentials: str, **kwargs) -> None:
        self.type = ConnectionType.bigquery
        self.name = name
        self.credentials = self._convert_json_if_needed(credentials)
        self.project_id = self.credentials["project_id"]

    def to_dict(self):
        """Dict for use with the BigQuery connector"""
        return {"credentials": self.credentials, "project_id": self.project_id}

    @staticmethod
    def _convert_json_if_needed(creds: dict):
        if isinstance(creds, dict):
            return deepcopy(creds)
        elif isinstance(creds, str):
            return json.loads(creds)
        else:
            raise TypeError(f"BigQuery credentials json had wrong type: {type(creds)} for value {creds}")
