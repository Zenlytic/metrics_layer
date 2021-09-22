import json
from copy import deepcopy


class BaseConnection:
    def __repr__(self):
        return f"<{self.__name__} name={self.name}>"


class SnowflakeConnection(BaseConnection):
    def __init__(
        self,
        name: str,
        account: str,
        username: str,
        password: str,
        role: str = None,
        warehouse: str = None,
        datebase: str = None,
        schema: str = None,
    ) -> None:
        self.type = "SNOWFLAKE"
        self.name = name
        self.account = account
        self.username = username
        self.password = password
        self.role = role
        self.warehouse = warehouse
        self.datebase = datebase
        self.schema = schema

    def to_dict(self):
        """Dict for use with the snowflake connector"""
        return {
            "user": self.username,
            "password": self.password,
            "account": self.account,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
            "role": self.role,
        }


class BigQueryConnection(BaseConnection):
    def __init__(self, name: str, creds_json: str) -> None:
        self.type = "BIGQUERY"
        self.name = name
        self.creds_json = self._convert_json_if_needed(creds_json)
        self.project_id = self.creds_json["project_id"]

    def to_dict(self):
        """Dict for use with the BigQuery connector"""
        return {"credentials": self.creds_json, "project_id": self.project_id}

    @staticmethod
    def _convert_json_if_needed(creds: dict):
        if isinstance(creds, dict):
            return deepcopy(creds)
        elif isinstance(creds, str):
            return json.loads(creds)
        else:
            raise TypeError(f"BigQuery credentials json had wrong type: {type(creds)} for value {creds}")
