import os
from copy import copy

import pandas as pd
import pytest
from click.testing import CliRunner

from metrics_layer.cli import debug, init, list_, seed, show, validate
from metrics_layer.cli.seeding import SeedMetricsLayer
from metrics_layer.core import MetricsLayerConnection
from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.parse.project_reader_base import ProjectReaderBase


@pytest.mark.cli
def test_cli_init(mocker, monkeypatch):
    yaml_dump_called = False

    def assert_called(data, path):
        nonlocal yaml_dump_called
        yaml_dump_called = True
        assert isinstance(data, dict)
        assert data["view-paths"] == ["views"]
        assert data["model-paths"] == ["models"]
        assert data["dashboard-paths"] == ["dashboards"]

    mocker.patch("os.mkdir")
    monkeypatch.setattr(ProjectReaderBase, "dump_yaml_file", assert_called)
    runner = CliRunner()
    result = runner.invoke(init)

    assert result.exit_code == 0
    dirs = ["views", "dashboards", "models"]
    for dir_path in dirs:
        call = os.path.join(os.getcwd(), dir_path)
        os.mkdir.assert_any_call(call)
    assert yaml_dump_called


@pytest.mark.cli
@pytest.mark.parametrize(
    "query_type,profile,target,database_override",
    [
        (Definitions.snowflake, None, None, None),
        (Definitions.databricks, None, None, None),
        (Definitions.databricks, None, None, "segment_events"),
        (Definitions.bigquery, None, None, None),
        (Definitions.postgres, None, None, None),
        (Definitions.postgres, None, None, "segment_events"),
        (Definitions.postgres, "alternative_demo", "alternative_target", None),
        (Definitions.redshift, None, None, None),
        (Definitions.druid, None, None, None),
        (Definitions.trino, None, None, None),
        (Definitions.sql_server, None, None, None),
        (Definitions.azure_synapse, None, None, None),
    ],
)
def test_cli_seed_metrics_layer(
    mocker,
    monkeypatch,
    connection,
    query_type,
    profile,
    target,
    database_override,
    seed_snowflake_tables_data,
    seed_bigquery_tables_data,
    seed_redshift_tables_data,
    seed_postgres_tables_data,
    seed_druid_tables_data,
    seed_trino_tables_data,
    seed_sql_server_tables_data,
    seed_databricks_tables_data,
):
    mocker.patch("os.mkdir")
    yaml_dump_called = 0

    def query_runner_mock(slf, query):
        print(query)
        if query_type == Definitions.snowflake and ".COLUMNS" in query:
            return seed_snowflake_tables_data
        elif query_type == Definitions.snowflake and ".TABLES" in query:
            return pd.DataFrame(
                [
                    {"TABLE_SCHEMA": "ANALYTICS", "TABLE_NAME": "ORDERS", "COMMENT": "orders table, bro"},
                    {"TABLE_SCHEMA": "ANALYTICS", "TABLE_NAME": "SESSIONS", "COMMENT": None},
                ]
            )
        elif query_type == Definitions.databricks and ".TABLES" in query:
            return pd.DataFrame(
                [
                    {"TABLE_SCHEMA": "analytics", "TABLE_NAME": "orders", "COMMENT": "orders table, bro"},
                    {"TABLE_SCHEMA": "analytics", "TABLE_NAME": "sessions", "COMMENT": None},
                ]
            )
        elif query_type == Definitions.redshift:
            return seed_redshift_tables_data
        elif query_type == Definitions.postgres:
            return seed_postgres_tables_data
        elif query_type == Definitions.bigquery:
            return seed_bigquery_tables_data
        elif query_type == Definitions.druid:
            return seed_druid_tables_data
        elif query_type == Definitions.trino:
            return seed_trino_tables_data
        elif query_type in {Definitions.sql_server, Definitions.azure_synapse}:
            return seed_sql_server_tables_data
        elif query_type == Definitions.databricks:
            return seed_databricks_tables_data
        raise ValueError("Query error, does not match expected")

    def yaml_dump_assert(slf, data, file):
        nonlocal yaml_dump_called
        yaml_dump_called += 1
        if "zenlytic_project.yml" in file:
            assert data["view-paths"] == ["views"]
            assert data["model-paths"] == ["models"]
            assert data["dashboard-paths"] == ["dashboards"]

        elif data["type"] == "model":
            assert data["name"] == "base_model"
            assert data["connection"] == "testing_snowflake"

        elif data["type"] == "view" and data["name"] == "orders":
            assert data["model_name"] == "base_model"
            if query_type in {Definitions.snowflake, Definitions.databricks}:
                assert data["description"] == "orders table, bro"
            if query_type in {Definitions.snowflake, Definitions.redshift}:
                assert data["sql_table_name"] == "ANALYTICS.ORDERS"
            if query_type in {Definitions.druid}:
                assert data["sql_table_name"] == "druid.orders"
            elif query_type == Definitions.bigquery:
                assert data["sql_table_name"] == "`analytics.analytics.orders`"
            elif (
                query_type
                in {
                    Definitions.postgres,
                    Definitions.azure_synapse,
                    Definitions.sql_server,
                    Definitions.databricks,
                    Definitions.trino,
                }
                and database_override is None
            ):
                assert data["sql_table_name"] == "analytics.orders"
            elif (
                query_type
                in {
                    Definitions.postgres,
                    Definitions.azure_synapse,
                    Definitions.sql_server,
                    Definitions.databricks,
                }
                and database_override
            ):
                assert data["sql_table_name"] == "segment_events.analytics.orders"
            assert "row_label" not in data

            order_id = next((f for f in data["fields"] if f["name"] == "order_id"))
            date = next((f for f in data["fields"] if f["name"] == "order_created_at"))
            new = next((f for f in data["fields"] if f["name"] == "new_vs_repeat"))
            num = next((f for f in data["fields"] if f["name"] == "revenue"))
            social = next((f for f in data["fields"] if f["name"] == "on_social_network"))
            acq_date = next((f for f in data["fields"] if f["name"] == "acquisition_date"))

            if query_type in {Definitions.snowflake, Definitions.databricks}:
                assert order_id["description"] == "I am an order id"

            assert social["type"] == "yesno"
            if query_type == Definitions.databricks:
                assert social["sql"] == "${TABLE}.on_social_network"
            elif query_type in {
                Definitions.snowflake,
                Definitions.druid,
                Definitions.duck_db,
                Definitions.postgres,
                Definitions.trino,
                Definitions.redshift,
                Definitions.sql_server,
                Definitions.azure_synapse,
            }:
                assert social["sql"] == '${TABLE}."ON_SOCIAL_NETWORK"'
            else:
                assert social["sql"] == "${TABLE}.ON_SOCIAL_NETWORK"

            assert acq_date["type"] == "time"
            if query_type in {Definitions.sql_server, Definitions.azure_synapse}:
                assert acq_date["datatype"] == "datetime"
            else:
                assert acq_date["datatype"] == "timestamp"
            if query_type == Definitions.databricks:
                assert acq_date["sql"] == "${TABLE}.acquisition_date"
            elif query_type in {
                Definitions.snowflake,
                Definitions.druid,
                Definitions.duck_db,
                Definitions.postgres,
                Definitions.trino,
                Definitions.redshift,
                Definitions.sql_server,
                Definitions.azure_synapse,
            }:
                assert acq_date["sql"] == '${TABLE}."ACQUISITION_DATE"'
            else:
                assert acq_date["sql"] == "${TABLE}.ACQUISITION_DATE"

            assert date["type"] == "time"
            if query_type in {
                Definitions.snowflake,
                Definitions.redshift,
                Definitions.postgres,
                Definitions.druid,
                Definitions.sql_server,
                Definitions.azure_synapse,
                Definitions.trino,
            }:
                assert date["datatype"] == "date"
            else:
                assert date["datatype"] == "timestamp"
            assert date["timeframes"] == [
                "raw",
                "date",
                "day_of_year",
                "week",
                "week_of_year",
                "month",
                "month_of_year",
                "quarter",
                "year",
            ]
            if query_type in {
                Definitions.snowflake,
                Definitions.druid,
                Definitions.duck_db,
                Definitions.postgres,
                Definitions.trino,
                Definitions.redshift,
                Definitions.sql_server,
                Definitions.azure_synapse,
            }:
                assert date["sql"] == '${TABLE}."ORDER_CREATED_AT"'
            else:
                assert date["sql"].upper() == "${TABLE}.ORDER_CREATED_AT"

            assert new["type"] == "string"
            if query_type in {
                Definitions.snowflake,
                Definitions.druid,
                Definitions.duck_db,
                Definitions.postgres,
                Definitions.trino,
                Definitions.redshift,
                Definitions.sql_server,
                Definitions.azure_synapse,
            }:
                assert new["sql"] == '${TABLE}."NEW_VS_REPEAT"'
            else:
                assert new["sql"].upper() == "${TABLE}.NEW_VS_REPEAT"

            assert num["type"] == "number"
            if query_type in {
                Definitions.snowflake,
                Definitions.druid,
                Definitions.duck_db,
                Definitions.postgres,
                Definitions.trino,
                Definitions.redshift,
                Definitions.sql_server,
                Definitions.azure_synapse,
            }:
                assert num["sql"] == '${TABLE}."REVENUE"'
            else:
                assert num["sql"].upper() == "${TABLE}.REVENUE"

            assert len(data["fields"]) == 14
            assert all(f["field_type"] != "measure" for f in data["fields"])
        elif data["type"] == "view" and data["name"] == "sessions":
            if query_type in {Definitions.snowflake, Definitions.redshift}:
                assert data["sql_table_name"] == "ANALYTICS.SESSIONS"
            elif query_type == Definitions.bigquery:
                assert data["sql_table_name"] == "`analytics.analytics.sessions`"
            elif (
                query_type
                in {
                    Definitions.postgres,
                    Definitions.sql_server,
                    Definitions.azure_synapse,
                    Definitions.databricks,
                    Definitions.trino,
                }
                and database_override is None
            ):
                assert data["sql_table_name"] == "analytics.sessions"
            elif (
                query_type
                in {
                    Definitions.postgres,
                    Definitions.sql_server,
                    Definitions.azure_synapse,
                    Definitions.databricks,
                }
                and database_override
            ):
                assert data["sql_table_name"] == "segment_events.analytics.sessions"
            assert "row_label" not in data

            date = next((f for f in data["fields"] if f["name"] == "session_date"))
            pk = next((f for f in data["fields"] if f["name"] == "session_id"))
            num = next((f for f in data["fields"] if f["name"] == "conversion"))
            cross_sell = next((f for f in data["fields"] if f["name"] == "crossell_product"))

            assert cross_sell["name"] == "crossell_product"
            if query_type in {
                Definitions.snowflake,
                Definitions.druid,
                Definitions.duck_db,
                Definitions.postgres,
                Definitions.trino,
                Definitions.redshift,
                Definitions.sql_server,
                Definitions.azure_synapse,
            }:
                assert cross_sell["sql"] == '${TABLE}."@CRoSSell P-roduct:"'
            else:
                assert cross_sell["sql"] == "${TABLE}.@CRoSSell P-roduct:"

            assert date["type"] == "time"
            if query_type in {
                Definitions.snowflake,
                Definitions.redshift,
                Definitions.postgres,
                Definitions.druid,
                Definitions.sql_server,
                Definitions.azure_synapse,
                Definitions.databricks,
                Definitions.trino,
            }:
                assert date["datatype"] == "date"
            else:
                assert date["datatype"] == "timestamp"
            assert date["timeframes"] == [
                "raw",
                "date",
                "day_of_year",
                "week",
                "week_of_year",
                "month",
                "month_of_year",
                "quarter",
                "year",
            ]
            if query_type in {
                Definitions.snowflake,
                Definitions.druid,
                Definitions.duck_db,
                Definitions.postgres,
                Definitions.trino,
                Definitions.redshift,
                Definitions.sql_server,
                Definitions.azure_synapse,
            }:
                assert date["sql"] == '${TABLE}."SESSION_DATE"'
            else:
                assert date["sql"].upper() == "${TABLE}.SESSION_DATE"

            assert pk["type"] == "string"
            if query_type in {
                Definitions.snowflake,
                Definitions.druid,
                Definitions.duck_db,
                Definitions.postgres,
                Definitions.trino,
                Definitions.redshift,
                Definitions.sql_server,
                Definitions.azure_synapse,
            }:
                assert pk["sql"] == '${TABLE}."SESSION_ID"'
            else:
                assert pk["sql"].upper() == "${TABLE}.SESSION_ID"

            assert num["type"] == "number"
            if query_type in {
                Definitions.snowflake,
                Definitions.druid,
                Definitions.duck_db,
                Definitions.postgres,
                Definitions.trino,
                Definitions.redshift,
                Definitions.sql_server,
                Definitions.azure_synapse,
            }:
                assert num["sql"] == '${TABLE}."CONVERSION"'
            else:
                assert num["sql"].upper() == "${TABLE}.CONVERSION"

            assert len(data["fields"]) == 14
            assert all(f["field_type"] != "measure" for f in data["fields"])
        else:
            raise AssertionError("undefined model type for seeding")

    old_type = copy(connection._raw_connections[0].type)
    connection._raw_connections[0].type = query_type

    def mock_init_profile(profile, target):
        assert target == target
        if target is None:
            assert profile == "demo"
        else:
            assert profile == "alternative_demo"
        return connection

    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", mock_init_profile)

    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")
    monkeypatch.setattr(SeedMetricsLayer, "run_query", query_runner_mock)
    monkeypatch.setattr(ProjectReaderBase, "dbt_project", None)
    monkeypatch.setattr(ProjectReaderBase, "dump_yaml_file", yaml_dump_assert)

    runner = CliRunner()
    result = runner.invoke(
        seed,
        [
            "--database",
            "analytics" if database_override is None else database_override,
            "--schema",
            "analytics" if query_type != Definitions.druid else "druid",
            "--connection",
            "testing_snowflake",
            "--profile",
            profile,
            "--target",
            target,
        ],
    )

    print(result)
    assert result.exit_code == 0
    dirs = ["views", "models", "dashboards"]
    calls = [os.path.join(os.getcwd(), dir_path) for dir_path in dirs]
    for call in calls:
        os.mkdir.assert_any_call(call)

    assert yaml_dump_called == 4

    runner = CliRunner()
    result = runner.invoke(
        seed,
        [
            "--database",
            "analytics" if database_override is None else database_override,
            "--schema",
            "analytics" if query_type != Definitions.druid else "druid",
            "--table",
            "orders",
            "--connection",
            "testing_snowflake",
        ],
    )

    connection._raw_connections[0].type = old_type
    assert result.exit_code == 0
    assert yaml_dump_called == 7


@pytest.mark.cli
def test_cli_validate(connection, fresh_project, mocker):
    mocker.patch(
        "metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: connection
    )
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")
    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == "Project passed (checked 2 models)!\n"

    # Break something so validation fails
    project = fresh_project
    project._views[1]["default_date"] = "sessions.session_date"
    sorted_fields = sorted(project._views[1]["fields"], key=lambda x: x["name"])
    sorted_fields[20]["name"] = "rev_broken_dim"
    project._views[1]["fields"] = sorted_fields
    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    # assert result.exit_code == 0
    assert (
        result.output
        == "Found 7 errors in the project:\n\n"
        "\nCould not locate reference revenue_dimension in field total_item_costs in view order_lines\n\n"
        "\nField total_item_costs in view order_lines contains invalid field reference revenue_dimension.\n\n"
        "\nCould not locate reference revenue_dimension in field revenue_in_cents in view orders\n\n"
        "\nCould not locate reference revenue_dimension in field total_revenue in view orders\n\n"
        "\nDefault date sessions.session_date in view orders does not exist.\n\n"
        "\nField revenue_in_cents in view orders contains invalid field reference revenue_dimension.\n\n"
        "\nField total_revenue in view orders contains invalid field reference revenue_dimension.\n\n"
    )


@pytest.mark.cli
def test_cli_validate_broken_canon_date(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    project._views[2]["fields"][-3]["canon_date"] = "does_not_exist"
    project.refresh_cache()
    project.join_graph

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert (
        result.output
        == "Found 1 error in the project:\n\n"
        "\nCanon date customers.does_not_exist is unreachable in field total_sessions.\n\n"
    )


@pytest.mark.cli
def test_cli_validate_personal_field(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    project._views[2]["fields"][2]["is_personal_field"] = True
    project._views[2]["fields"][2].pop("type")

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert (
        result.output
        == "Found 2 errors in the project:\n\n\nWarning: Field cancelled in view customers is missing the"
        " required key 'type'.\n\n\nWarning: Field cancelled in view customers has an invalid type None."
        " Valid types for dimension groups are: ['time', 'duration']\n\n"
    )


@pytest.mark.cli
def test_cli_validate_personal_field_view_level_error(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    project._views[2]["fields"][2]["is_personal_field"] = True
    project._views[2]["fields"][2]["sql"] = "${some_crazy_ref}"

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 2 errors in the project:\n\n"
        "\nWarning: Could not locate reference some_crazy_ref in field cancelled in view customers\n\n"  # noqa
        "\nWarning: Field cancelled in view customers contains invalid field reference some_crazy_ref.\n\n"  # noqa
    )


@pytest.mark.cli
def test_cli_validate_joins(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    identifiers = project._views[1]["identifiers"]
    identifiers[1]["sql_on"] = "${discounts.order_id}=${orders.wrong_name_order_id}"
    project._views[1]["identifiers"] = identifiers

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 2 errors in the project:\n\n"
        "\nCould not find field wrong_name_order_id in join between orders and discounts referencing view orders\n\n"  # noqa
        "\nCould not find field wrong_name_order_id in join between discounts and orders referencing view orders\n\n"  # noqa
    )


@pytest.mark.cli
def test_cli_validate_access_grants_setup(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project

    # This tests the scenario  an access filter is specified like a dict not a list
    # access_filters:
    #  user_attribute: 'department'
    #  allowed_values: ["finance"]
    #
    # ^incorrect syntax
    project._views[1]["access_filters"] = {"user_attribute": "department", "allowed_values": ["finance"]}

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 1 error in the project:\n\n"
        "\nThe view orders has an access filter, {'user_attribute': 'department', 'allowed_values': ['finance']} that is incorrectly specified as a when it should be a list, to specify it correctly check the documentation for access filters at https://docs.zenlytic.com/docs/data_modeling/access_grants#access-filters\n\n"  # noqa
    )


@pytest.mark.cli
def test_cli_validate_warnings_for_no_date_on_metrics(connection, fresh_project, mocker):
    # Tests removing the default date and raising a warning for the normal
    # metric and an error for the merged result
    project = fresh_project
    project._views[3].pop("default_date")

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 1 error in the project:\n\n"
        "\nField discount_per_order in view discounts is a merged result metric (measure), but does not have a date associated with it. Associate a date with the metric (measure) by setting either the canon_date property on the measure itself or the default_date property on the view the measure is in. Merged results are not possible without associated dates.\n\n"  # noqa
    )


@pytest.mark.cli
def test_cli_validate_default_date_is_dim_group(connection, fresh_project, mocker):
    # change the type of the default date to a non dim group
    project = fresh_project

    project._views[3]["default_date"] = "discount_code"

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 1 error in the project:\n\n"
        "\nDefault date discount_code is not of field_type: dimension_group and type: time in view discounts\n\n"  # noqa
    )


@pytest.mark.cli
def test_cli_validate_metric_self_reference(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project

    project._views[2]["fields"][3]["sql"] = "${number_of_customers}"

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 1 error in the project:\n\n"
        "\nField number_of_customers references itself in its 'sql' property. You need to reference a column using the ${TABLE}.myfield_name syntax or reference another dimension or measure.\n\n"  # noqa
    )


@pytest.mark.cli
def test_cli_validate_filter_with_no_field(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project

    project._views[2]["fields"][-3]["filters"][0] = {"is_churned": None, "value": False}

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")
    conn.project.validate()

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert (
        result.output
        == "Found 3 errors in the project:\n\n\nField total_sessions filter in View customers is missing the"
        " required field property\n\n\nField total_sessions filter in View customers has an invalid value"
        " property. Valid values can be found here in the docs:"
        " https://docs.zenlytic.com/docs/data_modeling/field_filter\n\n\nProperty is_churned is present"
        " on Field Filter in field total_sessions in view customers, but it is not a valid property.\n\n"
    )


@pytest.mark.cli
def test_cli_validate_dashboards(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    dashboards = sorted(project._dashboards, key=lambda x: x["name"])

    dashboards[0]["elements"][0]["slice_by"][0] = "missing_campaign"
    dashboards[0]["elements"][1]["metrics"][0] = "missing_revenue"
    project._dashboards = dashboards
    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 2 errors in the project:\n\n"
        "\nCould not find field missing_campaign referenced in dashboard sales_dashboard\n\n"  # noqa
        "\nCould not find field missing_revenue referenced in dashboard sales_dashboard\n\n"  # noqa
    )

    dashboards[0]["elements"][0]["explore"] = "order_lines_all"
    dashboards[0]["elements"][0]["slice_by"][0] = "orders.new_vs_repeat"
    project._dashboards = dashboards


@pytest.mark.cli
def test_cli_validate_names(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    sorted_fields = sorted(project._views[1]["fields"], key=lambda x: x["name"])

    sorted_fields[2]["name"] = "an invalid @name\\"
    sorted_fields[5]["timeframes"] = ["date", "month", "year"]
    project._views[1]["fields"] = sorted_fields
    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 4 errors in the project:\n\n"
        "\nCould not locate reference days_between_orders in field an invalid @name\\ in view orders\n\n"
        "\nField name: an invalid @name\\ is invalid. Please reference the naming conventions (only letters, numbers, or underscores)\n\n"  # noqa
        "\nField an invalid @name\ in view orders contains invalid field reference days_between_orders.\n\n"
        "\nField between_orders in view orders is of type duration, but has property timeframes when it should have property intervals\n\n"  # noqa
    )


@pytest.mark.cli
def test_cli_validate_model_name_in_view(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    project._views[1].pop("model_name")
    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert (
        result.output
        == "Found 1 error in the project:\n\n"
        "\nCould not find a model in the view orders. Use the model_name property to specify the model.\n\n"
    )


@pytest.mark.cli
def test_cli_validate_two_customer_tags(connection, fresh_project, mocker):
    # Break something so validation fails
    sorted_fields = sorted(fresh_project._views[1]["fields"], key=lambda x: x["name"])
    sorted_fields[7]["tags"] = ["customer"]
    conn = MetricsLayerConnection(project=fresh_project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert (
        result.output
        == "Found 2 errors in the project:\n\n\nMultiple fields found for the tag customer - those fields"
        " were ['orders.cumulative_aov', 'customers.customer_id']. Only one field can have the tag"
        ' "customer" per joinable graph.\n\n\nProperty tags is present on Field cumulative_aov in view'
        " orders, but it is not a valid property.\n\n"
    )


@pytest.mark.cli
def test_cli_dashboard_model_does_not_exist(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    dashboards = sorted(project._dashboards, key=lambda x: x["name"])

    dashboards[0]["elements"][0]["model"] = "missing_model"
    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert (
        result.output
        == "Found 1 error in the project:\n\n"
        "\nCould not find or you do not have access to model missing_model in dashboard sales_dashboard\n\n"
    )


@pytest.mark.cli
def test_cli_canon_date_inaccessible(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    sorted_fields = sorted(fresh_project._views[1]["fields"], key=lambda x: x["name"])
    sorted_fields[-2]["canon_date"] = "missing_field"

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert (
        result.output
        == "Found 1 error in the project:\n\n"
        "\nCanon date orders.missing_field is unreachable in field total_revenue.\n\n"
    )


@pytest.mark.cli
def test_cli_dimension_group_timeframes(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    sorted_fields = sorted(fresh_project._views[1]["fields"], key=lambda x: x["name"])
    date_idx = next((i for i, f in enumerate(sorted_fields) if f["name"] == "order"))
    sorted_fields[date_idx]["timeframes"] = ["raw", "timestamp", "date", "week", "month", "year"]

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 3 errors in the project:\n\n"
        "\nIn the Set test_set2 Field order_time not found in view orders, please check that this field exists AND that you have access to it. \n\nIf this is a dimension group specify the group parameter, if not already specified, for example, with a dimension group named 'order' with timeframes: [raw, date, month] specify 'order_raw' or 'order_date' or 'order_month'\n\n"  # noqa
        "\nIn the Set test_set_composed Field order_time not found in view orders, please check that this field exists AND that you have access to it. \n\nIf this is a dimension group specify the group parameter, if not already specified, for example, with a dimension group named 'order' with timeframes: [raw, date, month] specify 'order_raw' or 'order_date' or 'order_month'\n\n"  # noqa
        "\nField order in view orders is of type time and has timeframe value of 'timestamp' which is not a valid timeframes (valid timeframes are ['raw', 'time', 'second', 'minute', 'hour', 'date', 'week', 'month', 'quarter', 'year', 'fiscal_month', 'fiscal_quarter', 'fiscal_year', 'week_index', 'week_of_year', 'week_of_month', 'month_of_year', 'month_of_year_full_name', 'month_of_year_index', 'fiscal_month_index', 'fiscal_month_of_year_index', 'month_name', 'month_index', 'quarter_of_year', 'fiscal_quarter_of_year', 'hour_of_day', 'day_of_week', 'day_of_month', 'day_of_year'])\n\n"  # noqa
    )


@pytest.mark.cli
def test_cli_looker_parameter(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    sorted_fields = sorted(fresh_project._views[1]["fields"], key=lambda x: x["name"])
    sorted_fields[-2]["sql"] = "{% if time_._parameter_value == 'seconds' %}\n ${TABLE}.time_second"

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 1 error in the project:\n\n"
        "\nField total_revenue in view orders contains invalid SQL in property sql. Remove any Looker parameter references from the SQL.\n\n"  # noqa
    )


@pytest.mark.cli
def test_cli_invalid_join_sql_syntax(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    fresh_project._views[1]["identifiers"][0]["sql"] = "{order_id}"

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 1 error in the project:\n\n"
        '\nWarning: Identifier order_id in view orders is missing "${", are you sure you are using the reference syntax correctly?\n\n'  # noqa
    )


@pytest.mark.cli
def test_cli_duplicate_field_names(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    project._views[2]["fields"][2]["name"] = "number_of_customers"

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert (
        result.output
        == "Found 1 error in the project:\n\n"
        "\nDuplicate field names in view customers: number_of_customers\n\n"
    )


@pytest.mark.cli
def test_cli_duplicate_view_names(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    fresh_project._views[0]["name"] = "orders"

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert (
        result.output
        == "Found 1 error in the project:\n\n\nDuplicate view names found in your project for the name"
        " orders. Please make sure all view names are unique (note: join_as on identifiers will create a"
        " view under its that name and the name must be unique).\n\n"
    )


@pytest.mark.cli
def test_cli_duplicate_join_as_names(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    fresh_project._views[0]["identifiers"][0]["join_as"] = "parent_account"
    fresh_project = fresh_project.__init__(fresh_project._models, fresh_project._views)

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert (
        result.output
        == "Found 1 error in the project:\n\n\nDuplicate view names found in your project for the name"
        " parent_account. Please make sure all view names are unique (note: join_as on identifiers will"
        " create a view under its that name and the name must be unique).\n\n"
    )


@pytest.mark.cli
def test_cli_validate_required_access_filters(connection, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    project.set_required_access_filter_user_attributes(["products"])

    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    print(result)
    assert result.exit_code == 0
    assert (
        result.output
        == "Found 20 errors in the project:\n\n\nView order_lines does not have any access filters, but an"
        " access filter with user attribute products is required.\n\n\nView orders does not have an access"
        " filter with the required user attribute products\n\n\nView customers does not have any access"
        " filters, but an access filter with user attribute products is required.\n\n\nView discounts does"
        " not have any access filters, but an access filter with user attribute products is"
        " required.\n\n\nView discount_detail does not have any access filters, but an access filter with"
        " user attribute products is required.\n\n\nView country_detail does not have any access filters,"
        " but an access filter with user attribute products is required.\n\n\nView sessions does not have"
        " any access filters, but an access filter with user attribute products is required.\n\n\nView"
        " events does not have any access filters, but an access filter with user attribute products is"
        " required.\n\n\nView login_events does not have any access filters, but an access filter with"
        " user attribute products is required.\n\n\nView traffic does not have any access filters, but an"
        " access filter with user attribute products is required.\n\n\nView clicked_on_page does not have"
        " any access filters, but an access filter with user attribute products is required.\n\n\nView"
        " accounts does not have any access filters, but an access filter with user attribute products is"
        " required.\n\n\nView aa_acquired_accounts does not have any access filters, but an access filter"
        " with user attribute products is required.\n\n\nView z_customer_accounts does not have any access"
        " filters, but an access filter with user attribute products is required.\n\n\nView"
        " other_db_traffic does not have any access filters, but an access filter with user attribute"
        " products is required.\n\n\nView created_workspace does not have any access filters, but an"
        " access filter with user attribute products is required.\n\n\nView mrr does not have any access"
        " filters, but an access filter with user attribute products is required.\n\n\nView"
        " monthly_aggregates does not have any access filters, but an access filter with user attribute"
        " products is required.\n\n\nView child_account does not have any access filters, but an access"
        " filter with user attribute products is required.\n\n\nView parent_account does not have any"
        " access filters, but an access filter with user attribute products is required.\n\n"
    )


@pytest.mark.cli
def test_cli_debug(connection, mocker):
    def query_runner_mock(query, connection, run_pre_queries=True):
        assert query == "select 1 as id;"
        assert connection.name in {"testing_snowflake", "testing_bigquery", "testing_databricks"}
        assert not run_pre_queries
        return True

    connection.run_query = query_runner_mock
    mocker.patch(
        "metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: connection
    )
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(debug)

    profiles_dir = os.path.join(os.path.expanduser("~"), ".dbt", "profiles.yml")
    print(result.output)
    assert result.exit_code == 0
    non_workstation_dependent_correct = (
        f"Using profiles.yml file at {profiles_dir}\n\n"
        "Configuration:\n"
        "  profiles.yml file OK found and valid\n"
        "\nRequired dependencies:\n"
        "  git [OK found]\n"
        "\nConnections:\n"
        "  name: testing_snowflake\n"
        "  account: blahblah.us-east-1\n"
        "  user: paul\n"
        "  database: analytics\n"
        "  warehouse: compute_wh\n"
        "  role: reporting\n"
        "  name: testing_bigquery\n"
        "  type: BIGQUERY\n"
        "  project_id: fake-proj-id\n"
        "  name: testing_databricks\n"
        "  host: blah.cloud.databricks.com\n"
        "  http_path: paul/testing/now\n"
        "\nConnection testing_snowflake test: OK connection ok\n"
        "\nConnection testing_bigquery test: OK connection ok\n"
        "\nConnection testing_databricks test: OK connection ok\n"
    )

    non_workstation_dependent_output = "\n".join(result.output.split("\n")[3:])
    assert non_workstation_dependent_output == non_workstation_dependent_correct


@pytest.mark.parametrize(
    "object_type,extra_args",
    [
        ("models", []),
        ("connections", []),
        ("views", []),
        ("fields", ["--view", "discount_detail"]),
        ("dimensions", ["--view", "discounts"]),
        ("dimensions", ["--view", "discounts", "--show-hidden"]),
        ("metrics", ["--view", "discounts"]),
    ],
)
@pytest.mark.cli
def test_cli_list(connection, mocker, object_type: str, extra_args: list):
    mocker.patch(
        "metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: connection
    )
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(list_, extra_args + [object_type])

    result_lookup = {
        "models": "Found 2 models:\n\ntest_model\nnew_model\n",
        "connections": "Found 3 connections:\n\ntesting_snowflake\ntesting_bigquery\ntesting_databricks\n",
        "views": (  # noqa
            "Found 21"
            " views:\n\norder_lines\norders\ncustomers\ndiscounts\ndiscount_detail\ncountry_detail\nsessions\nevents\nlogin_events\ntraffic\nclicked_on_page\nsubmitted_form\naccounts\naa_acquired_accounts\nz_customer_accounts\nother_db_traffic\ncreated_workspace\nmrr\nmonthly_aggregates\nchild_account\nparent_account\n"  # noqa
        ),
        "fields": "Found 2 fields:\n\ndiscount_promo_name\ndiscount_usd\n",
        "dimensions": "Found 3 dimensions:\n\ncountry\norder\ndiscount_code\n",
        "metrics": "Found 2 metrics:\n\ntotal_discount_amt\ndiscount_per_order\n",  # noqa
    }

    print(extra_args)
    if any("show-hidden" in a for a in extra_args):
        correct = "Found 5 dimensions:\n\ndiscount_id\norder_id\ncountry\norder\ndiscount_code\n"  # noqa
    else:
        correct = result_lookup[object_type]

    print(result)
    assert result.exit_code == 0
    assert result.output == correct


@pytest.mark.parametrize(
    "name,extra_args",
    [
        ("test_model", ["--type", "model"]),
        ("testing_snowflake", ["--type", "connection"]),
        ("discounts", ["--type", "view"]),
        ("order_id", ["--type", "field", "--view", "orders"]),
        ("order_date", ["--type", "dimension", "--view", "discounts"]),
        ("total_discount_amt", ["--type", "metric"]),
    ],
)
@pytest.mark.cli
def test_cli_show(connection, mocker, name, extra_args):
    mocker.patch(
        "metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: connection
    )
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(show, extra_args + [name])

    result_lookup = {
        "test_model": (
            "Attributes in model test_model:\n\n"
            "  name: test_model\n"
            "  type: model\n"
            "  label: Test commerce data\n"
            "  connection: testing_snowflake\n"
        ),
        "testing_snowflake": (
            "Attributes in connection testing_snowflake:\n\n"
            "  name: testing_snowflake\n"
            "  account: blahblah.us-east-1\n"
            "  user: paul\n"
            "  database: analytics\n"
            "  warehouse: compute_wh\n"
            "  role: reporting\n"
        ),
        "discounts": (
            "Attributes in view discounts:\n\n"
            "  name: discounts\n"
            "  type: view\n"
            "  sql_table_name: analytics_live.discounts\n"
            "  number_of_fields: 7\n"
        ),
        "order_id": (
            "Attributes in field order_id:\n\n"
            "  name: order_id\n"
            "  field_type: dimension\n"
            "  type: string\n"
            "  group_label: ID's\n"
            "  hidden: True\n"
            "  primary_key: True\n"
            "  sql: ${TABLE}.id\n"
        ),
        "order_date": (
            "Attributes in dimension order_date:\n\n"
            "  name: order\n"
            "  field_type: dimension_group\n"
            "  type: time\n"
            "  timeframes:\n    date\n    week\n    month\n    year\n"
            "  sql: ${TABLE}.order_date\n"
        ),
        "total_discount_amt": (
            "Attributes in metric total_discount_amt:\n\n"
            "  name: total_discount_amt\n"
            "  field_type: measure\n"
            "  type: sum\n"
            "  sql: ${TABLE}.discount_amt\n"
        ),
    }

    print(result)
    assert result.exit_code == 0
    assert result.output == result_lookup[name]
