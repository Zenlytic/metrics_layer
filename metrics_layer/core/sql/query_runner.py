try:
    import snowflake.connector
except ModuleNotFoundError:
    pass

try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
except ModuleNotFoundError:
    pass

try:
    import redshift_connector
except ModuleNotFoundError:
    pass

from metrics_layer.core.parse.connections import (
    BaseConnection,
    ConnectionType,
    MetricsLayerConnectionError,
)


class QueryRunner:
    def __init__(self, query: str, connection: BaseConnection):
        self.query = query
        self.connection = connection
        self._query_runner_lookup = {
            ConnectionType.snowflake: self._run_snowflake_query,
            ConnectionType.bigquery: self._run_bigquery_query,
            ConnectionType.redshift: self._run_redshift_query,
        }
        if self.connection.type not in self._query_runner_lookup:
            supported = list(self._query_runner_lookup.keys())
            raise MetricsLayerConnectionError(
                f"Connection type {self.connection.type} not supported, supported types are {supported}"
            )

    # 3 min timeout default set in seconds (aborts query after timeout)
    def run_query(self, timeout: int = 180, **kwargs):
        query_runner = self._query_runner_lookup[self.connection.type]
        df = query_runner(
            timeout=timeout,
            raw_cursor=kwargs.get("raw_cursor", False),
            run_pre_queries=kwargs.get("run_pre_queries", True),
            start_warehouse=kwargs.get("start_warehouse", True),
        )
        return df

    def _run_snowflake_query(
        self, timeout: int, raw_cursor: bool, run_pre_queries: bool, start_warehouse: bool
    ):
        snowflake_connection = self._get_snowflake_connection(self.connection)
        if run_pre_queries:
            self._run_snowflake_pre_queries(snowflake_connection)
        elif start_warehouse:
            self._run_snowflake_pre_queries(snowflake_connection, warehouse_only=True)
        cursor = snowflake_connection.cursor()
        cursor.execute(self.query, timeout=timeout)

        snowflake_connection.close()
        if raw_cursor:
            return cursor
        df = cursor.fetch_pandas_all()
        return df

    def _run_redshift_query(
        self, timeout: int, raw_cursor: bool, run_pre_queries: bool, start_warehouse: bool
    ):
        redshift_connection = self._get_redshift_connection(self.connection, timeout=timeout)
        cursor = redshift_connection.cursor()
        cursor.execute(self.query)

        redshift_connection.close()
        if raw_cursor:
            return cursor
        df = cursor.fetch_dataframe()
        return df

    def _run_bigquery_query(
        self, timeout: int, raw_cursor: bool, run_pre_queries: bool, start_warehouse: bool
    ):
        bigquery_connection = self._get_bigquery_connection(self.connection)
        result = bigquery_connection.query(self.query, timeout=timeout, job_retry=None)
        bigquery_connection.close()
        if raw_cursor:
            return result
        df = result.to_dataframe()
        return df

    def _run_snowflake_pre_queries(self, snowflake_connection, warehouse_only: bool = False):
        to_execute = ""
        if self.connection.warehouse:
            to_execute += f"USE WAREHOUSE {self.connection.warehouse};"
        if self.connection.database and not warehouse_only:
            to_execute += f'USE DATABASE "{self.connection.database.upper()}";'
        if self.connection.schema and not warehouse_only:
            to_execute += f'USE SCHEMA "{self.connection.schema.upper()}";'

        if to_execute != "":
            snowflake_connection.execute_string(to_execute)

    @staticmethod
    def _get_snowflake_connection(connection: BaseConnection):
        try:
            return snowflake.connector.connect(
                account=connection.account,
                user=connection.username,
                password=connection.password,
                role=connection.role,
            )
        except (ModuleNotFoundError, NameError):
            raise ModuleNotFoundError(
                "MetricsLayer could not find the Snowflake modules it needs to run the query. "
                "Make sure that you have those modules installed or reinstall MetricsLayer with "
                "the [snowflake] option e.g. pip install metrics-layer[snowflake]"
            )

    @staticmethod
    def _get_redshift_connection(connection: BaseConnection, timeout: int = None):
        try:
            return redshift_connector.connect(
                host=connection.host,
                port=connection.port,
                database=connection.database,
                user=connection.username,
                password=connection.password,
                timeout=timeout,
            )
        except (ModuleNotFoundError, NameError):
            raise ModuleNotFoundError(
                "MetricsLayer could not find the Redshift modules it needs to run the query. "
                "Make sure that you have those modules installed or reinstall MetricsLayer with "
                "the [redshift] option e.g. pip install metrics-layer[redshift]"
            )

    @staticmethod
    def _get_bigquery_connection(connection: BaseConnection):
        try:
            service_account_creds = service_account.Credentials.from_service_account_info(
                connection.credentials
            )
            connection = bigquery.Client(project=connection.project_id, credentials=service_account_creds)
        except (ModuleNotFoundError, NameError):
            raise ModuleNotFoundError(
                "MetricsLayer could not find the BigQuery modules it needs to run the query. "
                "Make sure that you have those modules installed or reinstall MetricsLayer with "
                "the [bigquery] option e.g. pip install metrics-layer[bigquery]"
            )
        return connection
