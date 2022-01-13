import os

import pytest
import yaml
from click.testing import CliRunner

from metrics_layer.cli import debug, init, list_, seed, show, validate
from metrics_layer.cli.seeding import SeedMetricsLayer
from metrics_layer.core import MetricsLayerConnection


@pytest.mark.cli
def test_cli_init(mocker):
    mocker.patch("os.mkdir")
    runner = CliRunner()
    result = runner.invoke(init, [])

    assert result.exit_code == 0
    calls = [os.path.join(os.getcwd(), dir_path) for dir_path in ["views/", "models/"]]
    for call in calls:
        os.mkdir.assert_any_call(call)


@pytest.mark.cli
def test_cli_seed(mocker, monkeypatch, connection, seed_tables_data, seed_views_data, get_seed_columns_data):
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
            assert data["connection"] == "testing_snowflake"
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

    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: connection)
    monkeypatch.setattr(SeedMetricsLayer, "run_query", query_runner_mock)
    monkeypatch.setattr(yaml, "dump", yaml_dump_assert)
    runner = CliRunner()
    result = runner.invoke(seed, ["--database", "demo", "--schema", "analytics", "demo"])

    assert result.exit_code == 0
    calls = [os.path.join(os.getcwd(), dir_path) for dir_path in ["views/", "models/"]]
    for call in calls:
        os.mkdir.assert_any_call(call)

    assert yaml_dump_called


@pytest.mark.cli
def test_cli_validate(config, connection, project, mocker):
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: connection)

    runner = CliRunner()
    result = runner.invoke(validate, ["demo"])

    assert result.exit_code == 0
    assert result.output == "Project passed (checked 2 explores)!\n"

    # Break something so validation fails
    sorted_fields = sorted(project._views[1]["fields"], key=lambda x: x["name"])
    sorted_fields[11]["name"] = "rev_broken_dim"
    project._views[1]["fields"] = sorted_fields
    config.project = project
    conn = MetricsLayerConnection(config=config)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: conn)

    runner = CliRunner()
    result = runner.invoke(validate, ["demo"])

    assert result.exit_code == 0
    assert result.output == (
        "Found 2 errors in the project:\n\n"
        "\nCould not locate reference revenue_dimension in view order_lines in explore order_lines_all\n\n"
        "\nCould not locate reference revenue_dimension in view orders in explore order_lines_all\n\n"
    )


@pytest.mark.cli
def test_cli_validate_dimension(config, project, mocker):
    # Break something so validation fails
    sorted_fields = sorted(project._views[1]["fields"], key=lambda x: x["name"])

    sorted_fields[11]["name"] = "revenue_dimension"
    sorted_fields[2]["sql"] = "${customer_id}"
    project._views[1]["fields"] = sorted_fields
    config.project = project
    conn = MetricsLayerConnection(config=config)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: conn)

    runner = CliRunner()
    result = runner.invoke(validate, ["demo"])

    assert result.exit_code == 0
    assert result.output == (
        "Found 1 error in the project:\n\n"
        "\nThe field average_order_value_custom is a measure with type number, but it's sql references "
        "another field that is not a measure. Please correct this reference to the right measure.\n\n"
    )


@pytest.mark.cli
def test_cli_debug(connection, mocker, monkeypatch):
    def query_runner_mock(query, connection):
        assert query == "select 1 as id;"
        assert connection.name == "testing_snowflake"
        return True

    connection.run_query = query_runner_mock
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: connection)
    runner = CliRunner()
    result = runner.invoke(debug, ["demo"])

    assert result.exit_code == 0
    non_workstation_dependent_correct = (
        "Using profiles.yml file at test_profiles_file.yml\n\n"
        "Configuration:\n"
        "  profiles.yml file OK found and valid\n"
        "\nRequired dependencies:\n"
        "  git [OK found]\n"
        "\nConnection:\n"
        "  name: testing_snowflake\n"
        "  account: blahblah.us-east-1\n"
        "  user: paul\n"
        "  database: analytics\n"
        "  warehouse: compute_wh\n"
        "  role: reporting\n"
        "\nConnection testing_snowflake test: OK connection ok\n"
    )

    non_workstation_dependent_output = "\n".join(result.output.split("\n")[3:])
    assert non_workstation_dependent_output == non_workstation_dependent_correct


@pytest.mark.parametrize(
    "object_type,extra_args",
    [
        ("models", []),
        ("connections", []),
        ("explores", []),
        ("views", ["--explore", "discounts_only"]),
        ("fields", ["--explore", "discounts_only"]),
        ("dimensions", ["--explore", "discounts_only", "--view", "discounts"]),
        ("dimensions", ["--explore", "discounts_only", "--show-hidden"]),
        ("metrics", ["--explore", "discounts_only"]),
    ],
)
@pytest.mark.cli
def test_cli_list(connection, mocker, object_type: str, extra_args: list):
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: connection)

    runner = CliRunner()
    result = runner.invoke(list_, extra_args + ["--profile", "demo", object_type])

    result_lookup = {
        "models": "Found 1 model:\n\ntest_model\n",
        "connections": "Found 1 connection:\n\ntesting_snowflake\n",
        "explores": "Found 2 explores:\n\norder_lines_all\ndiscounts_only\n",
        "views": "Found 1 view:\n\ndiscounts\n",
        "fields": "Found 4 fields:\n\ncountry\norder\ndiscount_code\ntotal_discount_amt\n",
        "dimensions": "Found 3 dimensions:\n\ncountry\norder\ndiscount_code\n",
        "metrics": "Found 1 metric:\n\ntotal_discount_amt\n",
    }

    if any("show-hidden" in a for a in extra_args):
        correct = "Found 5 dimensions:\n\ndiscount_id\norder_id\ncountry\norder\ndiscount_code\n"
    else:
        correct = result_lookup[object_type]

    assert result.exit_code == 0
    assert result.output == correct


@pytest.mark.parametrize(
    "name,extra_args",
    [
        ("test_model", ["--type", "model"]),
        ("testing_snowflake", ["--type", "connection"]),
        ("discounts_only", ["--type", "explore"]),
        ("discounts", ["--type", "view", "--explore", "discounts_only"]),
        ("order_id", ["--type", "field", "--explore", "discounts_only"]),
        ("order_date", ["--type", "dimension", "--explore", "discounts_only", "--view", "discounts"]),
        ("total_discount_amt", ["--type", "metric", "--explore", "discounts_only"]),
    ],
)
@pytest.mark.cli
def test_cli_show(connection, mocker, name, extra_args):
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: connection)

    runner = CliRunner()
    result = runner.invoke(show, extra_args + ["--profile", "demo", name])

    result_lookup = {
        "test_model": (
            "Attributes in model test_model:\n\n"
            "  name: test_model\n"
            "  type: model\n"
            "  label: Test commerce data\n"
            "  connection: connection_name\n"
            "  explore_names:\n"
            "    order_lines_all\n"
            "    discounts_only\n"
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
        "discounts_only": (
            "Attributes in explore discounts_only:\n\n"
            "  name: discounts_only\n"
            "  type: explore\n"
            "  from: discounts\n"
            "  join_names: []\n"
        ),
        "discounts": (
            "Attributes in view discounts:\n\n"
            "  name: discounts\n"
            "  type: view\n"
            "  sql_table_name: analytics_live.discounts\n"
            "  number_of_fields: 6\n"
        ),
        "order_id": (
            "Attributes in field order_id:\n\n"
            "  name: order_id\n"
            "  field_type: dimension\n"
            "  type: string\n"
            "  group_label: ID's\n"
            "  hidden: yes\n"
            "  sql: ${TABLE}.order_id\n"
        ),
        "order_date": (
            "Attributes in dimension order_date:\n\n"
            "  name: order\n"
            "  field_type: dimension_group\n"
            "  type: time\n"
            "  timeframes:\n    raw\n    time\n    date\n    week\n    month\n    quarter\n    year\n"
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

    assert result.exit_code == 0
    assert result.output == result_lookup[name]
