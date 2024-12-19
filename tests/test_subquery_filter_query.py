# from datetime import datetime

import pytest

from metrics_layer.core.exceptions import (
    AccessDeniedOrDoesNotExistException,
    JoinError,
    QueryError,
)

# from metrics_layer.core.model import Definitions
# from metrics_layer.core.sql.query_errors import ParseError


@pytest.mark.query
def test_query_subquery_filter(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders"],
        dimensions=["region"],
        where=[
            {
                "conditional_filter_logic": {
                    "conditions": [
                        {
                            "field": "orders.order_id",
                            "expression": "is_in_query",
                            "value": {
                                "query": {
                                    "metrics": [],
                                    "dimensions": ["orders.order_id"],
                                    "where": [
                                        {
                                            "field": "channel",
                                            "expression": "contains_case_insensitive",
                                            "value": "social",
                                        }
                                    ],
                                },
                                "field": "orders.order_id",
                            },
                        },
                    ],
                    "logical_operator": "AND",
                }
            },
            {"field": "product_name", "expression": "not_equal_to", "value": "Shipping Protection"},
        ],
        having=[{"field": "total_item_revenue", "expression": "less_than", "value": 300_000}],
    )

    correct = (
        "WITH filter_subquery_0 AS (SELECT orders.id as orders_order_id FROM analytics.order_line_items"
        " order_lines LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id WHERE"
        " LOWER(order_lines.sales_channel) LIKE LOWER('%social%') GROUP BY orders.id ORDER BY orders_order_id"
        " ASC NULLS LAST) SELECT customers.region as customers_region,NULLIF(COUNT(DISTINCT CASE WHEN "
        " (orders.id)  IS NOT NULL THEN  orders.id  ELSE NULL END), 0) as orders_number_of_orders FROM"
        " analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id LEFT JOIN analytics.customers customers ON"
        " order_lines.customer_id=customers.customer_id WHERE orders.id IN (SELECT DISTINCT orders_order_id"
        " FROM filter_subquery_0) AND order_lines.product_name<>'Shipping Protection' GROUP BY"
        " customers.region HAVING SUM(order_lines.revenue)<300000 ORDER BY orders_number_of_orders DESC NULLS"
        " LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_subquery_filter_with_or_syntax(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders"],
        dimensions=["region"],
        where=[
            {
                "conditional_filter_logic": {
                    "conditions": [
                        {
                            "field": "orders.order_id",
                            "expression": "is_in_query",
                            "value": {
                                "query": {
                                    "metrics": [],
                                    "dimensions": ["orders.order_id"],
                                    "where": [
                                        {
                                            "field": "channel",
                                            "expression": "contains_case_insensitive",
                                            "value": "social",
                                        }
                                    ],
                                },
                                "field": "orders.order_id",
                            },
                        },
                        {
                            "field": "orders.order_id",
                            "expression": "is_in_query",
                            "value": {
                                "query": {
                                    "metrics": [],
                                    "dimensions": ["orders.order_id"],
                                    "where": [
                                        {
                                            "field": "channel",
                                            "expression": "contains_case_insensitive",
                                            "value": "email",
                                        },
                                        {
                                            "field": "orders.order_date",
                                            "expression": "greater_than",
                                            "value": "2024-01-01",
                                        },
                                        {
                                            "field": "orders.order_date",
                                            "expression": "less_than",
                                            "value": "2024-01-31",
                                        },
                                    ],
                                },
                                "field": "orders.order_id",
                            },
                        },
                        {
                            "field": "product_name",
                            "expression": "not_equal_to",
                            "value": "Shipping Protection",
                        },
                    ],
                    "logical_operator": "OR",
                }
            },
        ],
    )

    correct = (
        "WITH filter_subquery_0 AS (SELECT orders.id as orders_order_id FROM analytics.order_line_items"
        " order_lines LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id WHERE"
        " LOWER(order_lines.sales_channel) LIKE LOWER('%social%') GROUP BY orders.id ORDER BY orders_order_id"
        " ASC NULLS LAST) ,filter_subquery_1 AS (SELECT orders.id as orders_order_id FROM"
        " analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id WHERE LOWER(order_lines.sales_channel) LIKE LOWER('%email%')"
        " AND DATE_TRUNC('DAY', orders.order_date)>'2024-01-01' AND DATE_TRUNC('DAY',"
        " orders.order_date)<'2024-01-31' GROUP BY orders.id ORDER BY orders_order_id ASC NULLS LAST) SELECT"
        " customers.region as customers_region,NULLIF(COUNT(DISTINCT CASE WHEN  (orders.id)  IS NOT NULL THEN"
        "  orders.id  ELSE NULL END), 0) as orders_number_of_orders FROM analytics.order_line_items"
        " order_lines LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id LEFT JOIN"
        " analytics.customers customers ON order_lines.customer_id=customers.customer_id WHERE orders.id IN"
        " (SELECT DISTINCT orders_order_id FROM filter_subquery_0) OR orders.id IN (SELECT DISTINCT"
        " orders_order_id FROM filter_subquery_1) OR order_lines.product_name<>'Shipping Protection' GROUP BY"
        " customers.region ORDER BY orders_number_of_orders DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_subquery_filter_with_is_not_in_query(connection):
    query = connection.get_sql_query(
        metrics=["total_revenue"],
        dimensions=["new_vs_repeat"],
        where=[
            {
                "conditional_filter_logic": {
                    "conditions": [
                        {
                            "field": "orders.customer_id",
                            "expression": "is_not_in_query",
                            "value": {
                                "query": {
                                    "metrics": [],
                                    "dimensions": ["customers.customer_id"],
                                    "where": [{"field": "gender", "expression": "equal_to", "value": "F"}],
                                },
                                "field": "customers.customer_id",
                            },
                        },
                        {
                            "logical_operator": "OR",
                            "conditions": [
                                {"field": "date", "expression": "less_than", "value": "2023-09-02"},
                                {"field": "new_vs_repeat", "expression": "equal_to", "value": "New"},
                                {
                                    "field": "customers.customer_id",
                                    "expression": "is_in_query",
                                    "value": {
                                        "query": {
                                            "metrics": [],
                                            "dimensions": ["customers.customer_id"],
                                            "where": [
                                                {"field": "gender", "expression": "equal_to", "value": "M"}
                                            ],
                                        },
                                        "field": "customers.customer_id",
                                    },
                                },
                            ],
                        },
                    ],
                    "logical_operator": "AND",
                }
            },
        ],
    )

    correct = (
        "WITH filter_subquery_0 AS (SELECT customers.customer_id as customers_customer_id FROM"
        " analytics.customers customers WHERE customers.gender='F' GROUP BY customers.customer_id ORDER BY"
        " customers_customer_id ASC NULLS LAST) ,filter_subquery_1 AS (SELECT customers.customer_id as"
        " customers_customer_id FROM analytics.customers customers WHERE customers.gender='M' GROUP BY"
        " customers.customer_id ORDER BY customers_customer_id ASC NULLS LAST) SELECT orders.new_vs_repeat as"
        " orders_new_vs_repeat,SUM(orders.revenue) as orders_total_revenue FROM analytics.orders orders LEFT"
        " JOIN analytics.customers customers ON orders.customer_id=customers.customer_id WHERE"
        " orders.customer_id NOT IN (SELECT DISTINCT customers_customer_id FROM filter_subquery_0) AND"
        " (DATE_TRUNC('DAY', orders.order_date)<'2023-09-02' OR orders.new_vs_repeat='New' OR"
        " customers.customer_id IN (SELECT DISTINCT customers_customer_id FROM filter_subquery_1)) GROUP BY"
        " orders.new_vs_repeat ORDER BY orders_total_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize("apply_limit", [True, False])
def test_query_subquery_filter_limit_and_non_limit(connection, apply_limit):
    query = connection.get_sql_query(
        metrics=["number_of_orders"],
        dimensions=[],
        where=[
            {
                "conditional_filter_logic": {
                    "conditions": [
                        {
                            "field": "channel",
                            "expression": "is_in_query",
                            "value": {
                                "query": {
                                    "metrics": [],
                                    "dimensions": ["channel"],
                                    "having": [
                                        {
                                            "field": "orders.total_revenue",
                                            "expression": "greater_than",
                                            "value": 100000,
                                        }
                                    ],
                                    "order_by": [{"field": "orders.total_revenue", "expression": "desc"}],
                                    "limit": 3,
                                },
                                "apply_limit": apply_limit,  # defaults to True
                                "field": "channel",
                            },
                        },
                    ],
                    "logical_operator": "AND",
                }
            },
        ],
    )

    if apply_limit:
        correct = (
            "WITH filter_subquery_0 AS (SELECT order_lines.sales_channel as order_lines_channel FROM"
            " analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
            " order_lines.order_unique_id=orders.id GROUP BY order_lines.sales_channel HAVING"
            " COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) * (1000000 * 1.0)) AS"
            " DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') %"
            " 1.0e27)::NUMERIC(38, 0)) - SUM(DISTINCT (TO_NUMBER(MD5(orders.id),"
            " 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) /"
            " CAST((1000000*1.0) AS DOUBLE PRECISION), 0)>100000 ORDER BY orders_total_revenue ASC NULLS LAST"
            " LIMIT 3) SELECT NULLIF(COUNT(DISTINCT CASE WHEN  (orders.id)  IS NOT NULL THEN  orders.id  ELSE"
            " NULL END), 0) as orders_number_of_orders FROM analytics.order_line_items order_lines LEFT JOIN"
            " analytics.orders orders ON order_lines.order_unique_id=orders.id WHERE"
            " order_lines.sales_channel IN (SELECT DISTINCT order_lines_channel FROM filter_subquery_0) ORDER"
            " BY orders_number_of_orders DESC NULLS LAST;"
        )
    else:
        correct = (
            "WITH filter_subquery_0 AS (SELECT order_lines.sales_channel as order_lines_channel FROM"
            " analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
            " order_lines.order_unique_id=orders.id GROUP BY order_lines.sales_channel HAVING"
            " COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) * (1000000 * 1.0)) AS"
            " DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') %"
            " 1.0e27)::NUMERIC(38, 0)) - SUM(DISTINCT (TO_NUMBER(MD5(orders.id),"
            " 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) /"
            " CAST((1000000*1.0) AS DOUBLE PRECISION), 0)>100000 ORDER BY orders_total_revenue ASC NULLS"
            " LAST) SELECT NULLIF(COUNT(DISTINCT CASE WHEN  (orders.id)  IS NOT NULL THEN  orders.id  ELSE"
            " NULL END), 0) as orders_number_of_orders FROM analytics.order_line_items order_lines LEFT JOIN"
            " analytics.orders orders ON order_lines.order_unique_id=orders.id WHERE"
            " order_lines.sales_channel IN (SELECT DISTINCT order_lines_channel FROM filter_subquery_0) ORDER"
            " BY orders_number_of_orders DESC NULLS LAST;"
        )

    assert query == correct


@pytest.mark.query
def test_query_subquery_filter_with_mapping(connection):
    query = connection.get_sql_query(
        metrics=["number_of_sessions"],
        dimensions=[],
        where=[
            {
                "conditional_filter_logic": {
                    "conditions": [
                        {
                            "field": "campaign",
                            "expression": "is_in_query",
                            "value": {
                                "query": {
                                    "metrics": [],
                                    "dimensions": ["campaign", "orders.order_id"],
                                    "having": [
                                        {
                                            "field": "orders.total_revenue",
                                            "expression": "greater_than",
                                            "value": 100,
                                        }
                                    ],
                                },
                                "field": "campaign",
                            },
                        },
                    ],
                    "logical_operator": "AND",
                }
            },
        ],
    )

    correct = (
        "WITH filter_subquery_0 AS (SELECT orders.campaign as orders_campaign,orders.id as orders_order_id"
        " FROM analytics.orders orders GROUP BY orders.campaign,orders.id HAVING SUM(orders.revenue)>100"
        " ORDER BY orders_campaign ASC NULLS LAST) SELECT COUNT(sessions.id) as sessions_number_of_sessions"
        " FROM analytics.sessions sessions WHERE sessions.utm_campaign IN (SELECT DISTINCT orders_campaign"
        " FROM filter_subquery_0) ORDER BY sessions_number_of_sessions DESC NULLS LAST;"
    )

    assert query == correct


@pytest.mark.query
def test_query_subquery_filter_invalid_query(connection):
    with pytest.raises(TypeError) as exc_info:
        connection.get_sql_query(
            metrics=["number_of_orders"],
            dimensions=[],
            where=[
                {
                    "conditional_filter_logic": {
                        "conditions": [
                            {
                                "field": "channel",
                                "expression": "is_in_query",
                                "value": {
                                    "query": {
                                        "measures": [],
                                        "dims": ["channel"],
                                        "having": [
                                            {
                                                "field": "orders.total_revenue",
                                                "expression": "greater_than",
                                                "value": 100000,
                                            }
                                        ],
                                        "order_by": [{"field": "orders.total_revenue", "expression": "desc"}],
                                    },
                                    "field": "channel",
                                },
                            },
                        ],
                        "logical_operator": "AND",
                    }
                },
            ],
        )

    assert exc_info.value
    assert "missing 1 required positional argument: 'metrics'" in str(exc_info.value)


@pytest.mark.query
def test_query_subquery_filter_invalid_field(connection):
    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_sql_query(
            metrics=["number_of_orders"],
            dimensions=[],
            where=[
                {
                    "conditional_filter_logic": {
                        "conditions": [
                            {
                                "field": "channel",
                                "expression": "is_in_query",
                                "value": {
                                    "query": {
                                        "metrics": [],
                                        "dimensions": ["channel"],
                                        "having": [
                                            {
                                                "field": "orders.total_revenue",
                                                "expression": "greater_than",
                                                "value": 100000,
                                            }
                                        ],
                                        "order_by": [{"field": "orders.total_revenue", "expression": "desc"}],
                                    },
                                    "field": "orders.fake_field",
                                },
                            },
                        ],
                        "logical_operator": "AND",
                    }
                },
            ],
        )

    assert exc_info.value
    assert "Field fake_field not found in view orders" in str(exc_info.value)


@pytest.mark.query
def test_query_subquery_filter_invalid_type(connection):
    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(
            metrics=["number_of_orders"],
            dimensions=[],
            where=[
                {
                    "conditional_filter_logic": {
                        "conditions": [
                            {
                                "field": "channel",
                                "expression": "is_in_query",
                                "value": {
                                    "query": "select * from myquery",
                                },
                            },
                        ],
                        "logical_operator": "AND",
                    }
                },
            ],
        )

    assert exc_info.value
    assert "Subquery filter value for the key 'query' must be a dict" in str(exc_info.value)


@pytest.mark.query
def test_query_subquery_filter_field_not_in_query(connection):
    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(
            metrics=["number_of_orders"],
            dimensions=[],
            where=[
                {
                    "conditional_filter_logic": {
                        "conditions": [
                            {
                                "field": "orders.new_vs_repeat",
                                "expression": "is_in_query",
                                "value": {
                                    "query": {
                                        "metrics": ["orders.total_revenue"],
                                        "dimensions": ["orders.order_date"],
                                        "having": [
                                            {
                                                "field": "orders.new_vs_repeat",
                                                "expression": "equal_to",
                                                "value": "New",
                                            }
                                        ],
                                    },
                                    "field": "orders.new_vs_repeat",
                                },
                            },
                        ],
                        "logical_operator": "AND",
                    }
                },
            ],
        )

    assert exc_info.value
    assert "Field orders.new_vs_repeat not found in subquery dimensions" in str(exc_info.value)


@pytest.mark.query
def test_query_subquery_filter_nested_simple_case(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders"],
        dimensions=["region"],
        where=[
            {
                "conditional_filter_logic": {
                    "conditions": [
                        {
                            "field": "orders.order_id",
                            "expression": "is_in_query",
                            "value": {
                                "query": {
                                    "metrics": [],
                                    "dimensions": ["orders.order_id"],
                                    "where": [
                                        {
                                            "field": "orders.order_id",
                                            "expression": "is_in_query",
                                            "value": {
                                                "query": {
                                                    "metrics": [],
                                                    "dimensions": ["orders.order_id"],
                                                    "where": [
                                                        {
                                                            "field": "channel",
                                                            "expression": "contains_case_insensitive",
                                                            "value": "email",
                                                        }
                                                    ],
                                                },
                                                "field": "orders.order_id",
                                            },
                                        },
                                        {
                                            "field": "product_name",
                                            "expression": "not_equal_to",
                                            "value": "Shipping Protection",
                                        },
                                    ],
                                },
                                "field": "orders.order_id",
                            },
                        },
                    ],
                    "logical_operator": "AND",
                }
            },
        ],
    )

    correct = (
        "WITH filter_subquery_0 AS (WITH filter_subquery_2_0 AS (SELECT orders.id as orders_order_id FROM"
        " analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id WHERE LOWER(order_lines.sales_channel) LIKE LOWER('%email%')"
        " GROUP BY orders.id ORDER BY orders_order_id ASC NULLS LAST) SELECT orders.id as orders_order_id"
        " FROM analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id WHERE orders.id IN (SELECT DISTINCT orders_order_id FROM"
        " filter_subquery_2_0) AND order_lines.product_name<>'Shipping Protection' GROUP BY orders.id ORDER"
        " BY orders_order_id ASC NULLS LAST) SELECT customers.region as customers_region,COUNT(orders.id) as"
        " orders_number_of_orders FROM analytics.orders orders LEFT JOIN analytics.customers customers ON"
        " orders.customer_id=customers.customer_id WHERE orders.id IN (SELECT DISTINCT orders_order_id FROM"
        " filter_subquery_0) GROUP BY customers.region ORDER BY orders_number_of_orders DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_subquery_filter_nested(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders"],
        dimensions=["region"],
        where=[
            {
                "field": "orders.order_id",
                "expression": "is_in_query",
                "value": {
                    "query": {
                        "metrics": [],
                        "dimensions": ["orders.order_id"],
                        "where": [
                            {
                                "conditional_filter_logic": {
                                    "logical_operator": "AND",
                                    "conditions": [
                                        {
                                            "field": "orders.order_id",
                                            "expression": "is_in_query",
                                            "value": {
                                                "query": {
                                                    "metrics": [],
                                                    "dimensions": ["orders.order_id"],
                                                    "where": [
                                                        {
                                                            "field": "channel",
                                                            "expression": "contains_case_insensitive",
                                                            "value": "email",
                                                        }
                                                    ],
                                                },
                                                "field": "orders.order_id",
                                            },
                                        },
                                        {
                                            "field": "product_name",
                                            "expression": "not_equal_to",
                                            "value": "Shipping Protection",
                                        },
                                    ],
                                },
                            }
                        ],
                    },
                    "field": "orders.order_id",
                },
            },
        ],
    )

    correct = (
        "WITH filter_subquery_0 AS (WITH filter_subquery_1_0 AS (SELECT orders.id as orders_order_id FROM"
        " analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id WHERE LOWER(order_lines.sales_channel) LIKE LOWER('%email%')"
        " GROUP BY orders.id ORDER BY orders_order_id ASC NULLS LAST) SELECT orders.id as orders_order_id"
        " FROM analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id WHERE orders.id IN (SELECT DISTINCT orders_order_id FROM"
        " filter_subquery_1_0) AND order_lines.product_name<>'Shipping Protection' GROUP BY orders.id ORDER"
        " BY orders_order_id ASC NULLS LAST) SELECT customers.region as customers_region,COUNT(orders.id) as"
        " orders_number_of_orders FROM analytics.orders orders LEFT JOIN analytics.customers customers ON"
        " orders.customer_id=customers.customer_id WHERE orders.id IN (SELECT DISTINCT orders_order_id FROM"
        " filter_subquery_0) GROUP BY customers.region ORDER BY orders_number_of_orders DESC NULLS LAST;"
    )
    assert query == correct
