# import pytest
import os

import yaml
from click.testing import CliRunner

from metrics_layer.cli import init, seed, validate
from metrics_layer.cli.seeding import SeedMetricsLayer
from metrics_layer.core import MetricsLayerConnection


def test_cli_init(mocker):
    mocker.patch("os.mkdir")
    runner = CliRunner()
    result = runner.invoke(init, [])

    assert result.exit_code == 0
    calls = [os.path.join(os.getcwd(), dir_path) for dir_path in ["views/", "models/"]]
    for call in calls:
        os.mkdir.assert_any_call(call)


def test_cli_seed(mocker, monkeypatch, seed_tables_data, seed_views_data, get_seed_columns_data):
    mocker.patch("os.mkdir")
    yaml_dump_called = False

    def query_runner_mock(slf, query):
        print(query)
        if query == "show tables in schema demo.analytics;":
            return seed_tables_data
        elif query == "show views in schema demo.analytics;":
            return seed_views_data
        elif query == 'show columns in "DEMO"."ANALYTICS"."ORDERS";':
            return get_seed_columns_data("ORDERS")
        elif query == 'show columns in "DEMO"."ANALYTICS"."SESSIONS";':
            return get_seed_columns_data("SESSIONS")
        raise ValueError("Query error, does not match expected")

    def yaml_dump_assert(data, file):
        nonlocal yaml_dump_called
        yaml_dump_called = True
        if data["type"] == "model":
            assert data["name"] == "base_model"
            assert data["connection"] == "demo_snowflake"
            assert len(data["explores"]) == 2
        elif data["type"] == "view" and data["name"] == "orders":
            assert data["sql_table_name"] == "ANALYTICS.ORDERS"

            date = next((f for f in data["fields"] if f["name"] == "order_created_at"))
            new = next((f for f in data["fields"] if f["name"] == "new_vs_repeat"))
            num = next((f for f in data["fields"] if f["name"] == "revenue"))
            social = next((f for f in data["fields"] if f["name"] == "on_social_network"))
            acq_date = next((f for f in data["fields"] if f["name"] == "acquisition_date"))

            assert social["type"] == "yesno"
            assert social["sql"] == "${TABLE}.ON_SOCIAL_NETWORK"

            assert acq_date["type"] == "time"
            assert acq_date["datatype"] == "timestamp"
            assert acq_date["sql"] == "${TABLE}.ACQUISITION_DATE"

            assert date["type"] == "time"
            assert date["datatype"] == "date"
            assert date["timeframes"] == ["raw", "date", "week", "month", "quarter", "year"]
            assert date["sql"] == "${TABLE}.ORDER_CREATED_AT"

            assert new["type"] == "string"
            assert new["sql"] == "${TABLE}.NEW_VS_REPEAT"

            assert num["type"] == "number"
            assert num["sql"] == "${TABLE}.REVENUE"

            assert len(data["fields"]) == 15
        elif data["type"] == "view" and data["name"] == "sessions":
            assert data["sql_table_name"] == "ANALYTICS.SESSIONS"

            date = next((f for f in data["fields"] if f["name"] == "session_date"))
            pk = next((f for f in data["fields"] if f["name"] == "session_id"))
            num = next((f for f in data["fields"] if f["name"] == "conversion"))

            assert date["type"] == "time"
            assert date["datatype"] == "date"
            assert date["timeframes"] == ["raw", "date", "week", "month", "quarter", "year"]
            assert date["sql"] == "${TABLE}.SESSION_DATE"

            assert pk["type"] == "string"
            assert pk["sql"] == "${TABLE}.SESSION_ID"

            assert num["type"] == "number"
            assert num["sql"] == "${TABLE}.CONVERSION"

            assert len(data["fields"]) == 15
        else:
            raise AssertionError("undefined model type for seeding")

    monkeypatch.setattr(SeedMetricsLayer, "run_query", query_runner_mock)
    monkeypatch.setattr(yaml, "dump", yaml_dump_assert)
    runner = CliRunner()
    result = runner.invoke(seed, ["--database", "demo", "--schema", "analytics", "demo"])

    assert result.exit_code == 0
    calls = [os.path.join(os.getcwd(), dir_path) for dir_path in ["views/", "models/"]]
    for call in calls:
        os.mkdir.assert_any_call(call)

    assert yaml_dump_called


def test_cli_validate(config, project, mocker):
    runner = CliRunner()
    result = runner.invoke(validate, ["demo"])

    assert result.exit_code == 0
    assert result.output == "Project passed!\n"

    # Break something so validation fails
    sorted_fields = sorted(project._views[1]["fields"], key=lambda x: x["name"])
    sorted_fields[9]["name"] = "rev_broken_dim"
    project._views[1]["fields"] = sorted_fields
    config.project = project
    conn = MetricsLayerConnection(config=config)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: conn)

    runner = CliRunner()
    result = runner.invoke(validate, ["demo"])

    assert result.exit_code == 0
    assert result.output == (
        "Found 3 errors in the project:\n\n"
        "\nCould not locate reference order_id in view orders in explore order_lines\n\n"
        "\nCould not locate reference revenue_dimension in view order_lines in explore order_lines\n\n"
        "\nCould not locate reference revenue_dimension in view orders in explore order_lines\n\n"
    )
