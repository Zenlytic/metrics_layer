import os
from copy import copy

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
    "query_type,profile,target",
    [
        (Definitions.snowflake, None, None),
        (Definitions.bigquery, None, None),
        (Definitions.postgres, None, None),
        (Definitions.postgres, "alternative_demo", "alternative_target"),
        (Definitions.redshift, None, None),
    ],
)
def test_cli_seed_metrics_layer(
    mocker,
    monkeypatch,
    connection,
    query_type,
    profile,
    target,
    seed_snowflake_tables_data,
    seed_bigquery_tables_data,
    seed_redshift_tables_data,
    seed_postgres_tables_data,
):
    mocker.patch("os.mkdir")
    yaml_dump_called = 0

    def query_runner_mock(slf, query):
        print(query)
        if query_type == Definitions.snowflake:
            return seed_snowflake_tables_data
        elif query_type == Definitions.redshift:
            return seed_redshift_tables_data
        elif query_type == Definitions.postgres:
            return seed_postgres_tables_data
        elif query_type == Definitions.bigquery:
            return seed_bigquery_tables_data
        raise ValueError("Query error, does not match expected")

    def yaml_dump_assert(slf, data, file):
        nonlocal yaml_dump_called
        yaml_dump_called += 1
        if data["type"] == "model":
            assert data["name"] == "base_model"
            assert data["connection"] == "testing_snowflake"

        elif data["type"] == "view" and data["name"] == "orders":
            assert data["model_name"] == "base_model"
            if query_type in {Definitions.snowflake, Definitions.redshift}:
                assert data["sql_table_name"] == "ANALYTICS.ORDERS"
            elif query_type == Definitions.bigquery:
                assert data["sql_table_name"] == "`demo.analytics.orders`"
            elif query_type == Definitions.postgres:
                assert data["sql_table_name"] == "analytics.orders"

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
            if query_type in {Definitions.snowflake, Definitions.redshift, Definitions.postgres}:
                assert date["datatype"] == "date"
            else:
                assert date["datatype"] == "timestamp"
            assert date["timeframes"] == ["raw", "date", "week", "month", "quarter", "year"]
            assert date["sql"] == "${TABLE}.ORDER_CREATED_AT"

            assert new["type"] == "string"
            assert new["sql"] == "${TABLE}.NEW_VS_REPEAT"

            assert num["type"] == "number"
            assert num["sql"] == "${TABLE}.REVENUE"

            assert len(data["fields"]) == 15
        elif data["type"] == "view" and data["name"] == "sessions":
            if query_type in {Definitions.snowflake, Definitions.redshift}:
                assert data["sql_table_name"] == "ANALYTICS.SESSIONS"
            elif query_type == Definitions.bigquery:
                assert data["sql_table_name"] == "`demo.analytics.sessions`"
            elif query_type == Definitions.postgres:
                assert data["sql_table_name"] == "analytics.sessions"

            date = next((f for f in data["fields"] if f["name"] == "session_date"))
            pk = next((f for f in data["fields"] if f["name"] == "session_id"))
            num = next((f for f in data["fields"] if f["name"] == "conversion"))

            print(date)
            assert date["type"] == "time"
            if query_type in {Definitions.snowflake, Definitions.redshift, Definitions.postgres}:
                assert date["datatype"] == "date"
            else:
                assert date["datatype"] == "timestamp"
            assert date["timeframes"] == ["raw", "date", "week", "month", "quarter", "year"]
            assert date["sql"] == "${TABLE}.SESSION_DATE"

            assert pk["type"] == "string"
            assert pk["sql"] == "${TABLE}.SESSION_ID"

            assert num["type"] == "number"
            assert num["sql"] == "${TABLE}.CONVERSION"

            assert len(data["fields"]) == 15
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
            "demo",
            "--schema",
            "analytics",
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
    dirs = ["dashboards", "views", "models"]
    calls = [os.path.join(os.getcwd(), dir_path) for dir_path in dirs]
    for call in calls:
        os.mkdir.assert_any_call(call)

    assert yaml_dump_called == 3

    runner = CliRunner()
    result = runner.invoke(
        seed,
        [
            "--database",
            "demo",
            "--schema",
            "analytics",
            "--table",
            "orders",
            "--connection",
            "testing_snowflake",
        ],
    )

    connection._raw_connections[0].type = old_type
    assert result.exit_code == 0
    assert yaml_dump_called == 5


@pytest.mark.cli
def test_cli_validate(connection, fresh_project, mocker):
    mocker.patch(
        "metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: connection
    )
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")
    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == "Project passed (checked 1 model)!\n"

    # Break something so validation fails
    project = fresh_project
    project._views[1]["default_date"] = "sessions.session_date"
    sorted_fields = sorted(project._views[1]["fields"], key=lambda x: x["name"])
    sorted_fields[18]["name"] = "rev_broken_dim"
    project._views[1]["fields"] = sorted_fields
    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    # assert result.exit_code == 0
    assert result.output == (
        "Found 3 errors in the project:\n\n"
        "\nCould not locate reference revenue_dimension in view order_lines\n\n"
        "\nCould not locate reference revenue_dimension in view orders\n\n"
        "\nDefault date sessions.session_date is unreachable in view orders\n\n"
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

    sorted_fields[0]["name"] = "an invalid @name"
    sorted_fields[3]["timeframes"] = ["date", "month", "year"]
    project._views[1]["fields"] = sorted_fields
    conn = MetricsLayerConnection(project=project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 3 errors in the project:\n\n"
        "\nCould not locate reference days_between_orders in view orders\n\n"
        "\nField name: an invalid @name is invalid. Please reference the naming conventions (only letters, numbers, or underscores)\n\n"  # noqa
        "\nField between_orders is of type duration, but has property timeframes when it should have property intervals\n\n"  # noqa
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
    assert result.output == (
        "Found 1 error in the project:\n\n"
        "\nCould not find a model in view orders. Use the model_name property to specify the model.\n\n"
    )


@pytest.mark.cli
def test_cli_validate_two_customer_tags(connection, fresh_project, mocker):
    # Break something so validation fails
    sorted_fields = sorted(fresh_project._views[1]["fields"], key=lambda x: x["name"])
    sorted_fields[5]["tags"] = ["customer"]
    conn = MetricsLayerConnection(project=fresh_project, connections=connection._raw_connections[0])
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile, target: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 1 error in the project:\n\n"
        "\nMultiple fields found for the tag customer - those fields were ['orders.cumulative_aov',"
        " 'customers.customer_id']. Only one field can have the tag \"customer\" per joinable graph.\n\n"
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
    assert result.output == (
        "Found 1 error in the project:\n\n"
        "\nCould not find model missing_model referenced in dashboard sales_dashboard.\n\n"
    )


@pytest.mark.cli
def test_cli_debug(connection, mocker):
    def query_runner_mock(query, connection, run_pre_queries=True):
        assert query == "select 1 as id;"
        assert connection.name in {"testing_snowflake", "testing_bigquery"}
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
        "\nConnection testing_snowflake test: OK connection ok\n"
        "\nConnection testing_bigquery test: OK connection ok\n"
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
        "models": "Found 1 model:\n\ntest_model\n",
        "connections": "Found 2 connections:\n\ntesting_snowflake\ntesting_bigquery\n",
        "views": "Found 9 views:\n\norder_lines\norders\ncustomers\ndiscounts\ndiscount_detail\ncountry_detail\nsessions\nevents\ntraffic\n",  # noqa
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
            "  group_label: ID's\n"
            "  hidden: yes\n"
            "  primary_key: yes\n"
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
