import os
from copy import copy

import pytest
from click.testing import CliRunner

from metrics_layer.cli import debug, init, list_, seed, show, validate
from metrics_layer.cli.seeding import SeedMetricsLayer
from metrics_layer.core import MetricsLayerConnection
from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.parse.project_reader import ProjectReader


@pytest.mark.cli
def test_cli_init(mocker, monkeypatch):
    yaml_dump_called = False

    def assert_called(data, path):
        nonlocal yaml_dump_called
        yaml_dump_called = True
        assert isinstance(data, dict)
        assert data["folder"] == "data_model/"

    mocker.patch("os.mkdir")
    monkeypatch.setattr(ProjectReader, "_dump_yaml_file", assert_called)
    runner = CliRunner()
    result = runner.invoke(init)

    assert result.exit_code == 0
    dirs = ["data_model/", "data_model/views/", "data_model/models/"]
    for dir_path in dirs:
        call = os.path.join(os.getcwd(), dir_path)
        os.mkdir.assert_any_call(call)
    assert yaml_dump_called


@pytest.mark.cli
# TODO add redshift test
@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.bigquery])
def test_cli_seed(
    mocker, monkeypatch, connection, query_type, seed_snowflake_tables_data, seed_bigquery_tables_data
):
    mocker.patch("os.mkdir")
    yaml_dump_called = 0

    def query_runner_mock(slf, query):
        print(query)
        if query_type in {Definitions.snowflake, Definitions.redshift}:
            return seed_snowflake_tables_data
        elif query_type == Definitions.bigquery:
            return seed_bigquery_tables_data
        raise ValueError("Query error, does not match expected")

    def yaml_dump_assert(slf, data, file):
        nonlocal yaml_dump_called
        yaml_dump_called += 1
        if data["type"] == "model":
            assert data["name"] == "base_model"
            assert data["connection"] == "testing_snowflake"
            assert len(data["explores"]) in {2, 1}  # 2 for first test 1 for second
        elif data["type"] == "view" and data["name"] == "orders":
            if query_type in {Definitions.snowflake, Definitions.redshift}:
                assert data["sql_table_name"] == "ANALYTICS.ORDERS"
            else:
                assert data["sql_table_name"] == "`demo.analytics.orders`"

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
            if query_type in {Definitions.snowflake, Definitions.redshift}:
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
            else:
                assert data["sql_table_name"] == "`demo.analytics.sessions`"

            date = next((f for f in data["fields"] if f["name"] == "session_date"))
            pk = next((f for f in data["fields"] if f["name"] == "session_id"))
            num = next((f for f in data["fields"] if f["name"] == "conversion"))

            assert date["type"] == "time"
            if query_type in {Definitions.snowflake, Definitions.redshift}:
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

    old_type = copy(connection.config._connections[0].type)
    connection.config._connections[0].type = query_type
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: connection)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")
    monkeypatch.setattr(SeedMetricsLayer, "run_query", query_runner_mock)
    monkeypatch.setattr(ProjectReader, "_dump_yaml_file", yaml_dump_assert)

    class repo_mock:
        repo_path = os.path.join(os.getcwd(), "data_model/")

    # Set repo path to ref local repo
    connection.config.repo = repo_mock

    runner = CliRunner()
    result = runner.invoke(seed, ["--database", "demo", "--schema", "analytics"])

    assert result.exit_code == 0
    dirs = ["data_model/", "data_model/views/", "data_model/models/"]
    calls = [os.path.join(os.getcwd(), dir_path) for dir_path in dirs]
    for call in calls:
        os.mkdir.assert_any_call(call)

    assert yaml_dump_called == 3

    runner = CliRunner()
    result = runner.invoke(seed, ["--database", "demo", "--schema", "analytics", "--table", "orders"])

    connection.config._connections[0].type = old_type
    assert result.exit_code == 0
    assert yaml_dump_called == 5


@pytest.mark.cli
def test_cli_validate(config, connection, fresh_project, mocker):
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: connection)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")
    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == "Project passed (checked 2 explores)!\n"

    # Break something so validation fails
    project = fresh_project
    sorted_fields = sorted(project._views[1]["fields"], key=lambda x: x["name"])
    sorted_fields[12]["name"] = "rev_broken_dim"
    project._views[1]["fields"] = sorted_fields
    config.project = project
    conn = MetricsLayerConnection(config=config)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 2 errors in the project:\n\n"
        "\nCould not locate reference revenue_dimension in view order_lines in explore order_lines_all\n\n"
        "\nCould not locate reference revenue_dimension in view orders in explore order_lines_all\n\n"
    )


@pytest.mark.cli
def test_cli_validate_dbt_refs(config, fresh_project, mocker, manifest):
    # Break something so validation fails
    project = fresh_project
    project.manifest = {}
    project.manifest_exists = False

    config.project = project
    conn = MetricsLayerConnection(config=config)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 1 error in the project:\n\n"
        "\nCould not find a dbt project co-located with this project to resolve the dbt ref('customers') "
        "in view customers in explore order_lines_all\n\n"
    )


@pytest.mark.cli
def test_cli_validate_joins(config, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    explores = sorted(project._models[0]["explores"], key=lambda x: x["name"])
    joins = sorted(explores[-1]["joins"], key=lambda x: x["name"])
    joins[0]["sql_on"] = "${order_lines_all.order_id}=${all_orders.wrong_name_order_id}"

    explores[-1]["joins"] = joins

    project._models[0]["explores"] = explores
    config.project = project
    conn = MetricsLayerConnection(config=config)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 1 error in the project:\n\n"
        "\nCould not find field wrong_name_order_id in join all_orders "
        "referencing view orders in explore order_lines_all\n\n"
    )


@pytest.mark.cli
def test_cli_validate_explores(config, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    explores = sorted(project._models[0]["explores"], key=lambda x: x["name"])

    explores[-1]["from"] = "missing_view"
    project._models[0]["explores"] = explores
    config.project = project
    conn = MetricsLayerConnection(config=config)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 6 errors in the project:\n\n"
        "\nCould not find field customer_id in join customers referencing view "
        "missing_view in explore order_lines_all\n\n"
        "\nCould not find view missing_view in join all_orders\n\n"
        "\nView missing_view cannot be found in explore order_lines_all\n\n"
        "\nCould not find field order_lines.product_name in explore order_lines_all referenced in dashboard sales_dashboard\n\n"  # noqa
        "\nCould not find field order_lines.product_name in explore order_lines_all referenced in a filter in dashboard sales_dashboard\n\n"  # noqa
        "\nCould not find field order_lines.product_name in explore order_lines_all referenced in dashboard sales_dashboard_v2\n\n"  # noqa
    )

    explores[-1]["from"] = "order_lines"
    project._models[0]["explores"] = explores


@pytest.mark.cli
def test_cli_validate_dashboards(config, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    dashboards = sorted(project._dashboards, key=lambda x: x["name"])

    dashboards[0]["elements"][0]["explore"] = "orders"
    dashboards[0]["elements"][0]["slice_by"][0] = "missing_campaign"
    project._dashboards = dashboards
    config.project = project
    conn = MetricsLayerConnection(config=config)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 3 errors in the project:\n\n"
        "\nCould not find explore orders in model test_model referenced in dashboard sales_dashboard\n\n"
        "\nCould not find field missing_campaign in explore orders referenced in dashboard sales_dashboard\n\n"  # noqa
        "\nCould not find field order_lines.product_name in explore orders referenced in dashboard sales_dashboard\n\n"  # noqa
    )

    dashboards[0]["elements"][0]["explore"] = "order_lines_all"
    dashboards[0]["elements"][0]["slice_by"][0] = "orders.new_vs_repeat"
    project._dashboards = dashboards


@pytest.mark.cli
def test_cli_validate_names(config, fresh_project, mocker):
    # Break something so validation fails
    project = fresh_project
    sorted_fields = sorted(project._views[1]["fields"], key=lambda x: x["name"])

    sorted_fields[0]["name"] = "an invalid @name"
    project._views[1]["fields"] = sorted_fields
    config.project = project
    conn = MetricsLayerConnection(config=config)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: conn)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(validate)

    assert result.exit_code == 0
    assert result.output == (
        "Found 1 error in the project:\n\n"
        "\nField name: an invalid @name is invalid. Please reference the naming conventions (only letters, numbers, or underscores)\n\n"  # noqa
    )


@pytest.mark.cli
def test_cli_debug(connection, mocker):
    def query_runner_mock(query, connection, run_pre_queries=True):
        assert query == "select 1 as id;"
        assert connection.name == "testing_snowflake"
        assert not run_pre_queries
        return True

    connection.run_query = query_runner_mock
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer._init_profile", lambda profile: connection)
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(debug)

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
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(list_, extra_args + [object_type])

    result_lookup = {
        "models": "Found 1 model:\n\ntest_model\n",
        "connections": "Found 1 connection:\n\ntesting_snowflake\n",
        "explores": "Found 2 explores:\n\norder_lines_all\ndiscounts_only\n",
        "views": "Found 2 views:\n\ndiscounts\ndiscount_detail\n",
        "fields": "Found 5 fields:\n\ncountry\norder\ndiscount_code\ntotal_discount_amt\ndiscount_usd\n",  # noqa
        "dimensions": "Found 3 dimensions:\n\ncountry\norder\ndiscount_code\n",
        "metrics": "Found 2 metrics:\n\ntotal_discount_amt\ndiscount_usd\n",
    }

    if any("show-hidden" in a for a in extra_args):
        correct = "Found 5 dimensions:\n\ndiscount_id\norder_id\ncountry\norder\ndiscount_code\n"  # noqa
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
    mocker.patch("metrics_layer.cli.seeding.SeedMetricsLayer.get_profile", lambda *args: "demo")

    runner = CliRunner()
    result = runner.invoke(show, extra_args + [name])

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
            "  join_names:\n    discount_detail\n"
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
