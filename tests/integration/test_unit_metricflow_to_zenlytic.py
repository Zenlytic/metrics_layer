import pytest

from metrics_layer.integrations.metricflow.metricflow_to_zenlytic import (
    ZenlyticUnsupportedError,
    convert_mf_dimension_to_zenlytic_dimension,
    convert_mf_entity_to_zenlytic_identifier,
    convert_mf_measure_to_zenlytic_measure,
    convert_mf_metric_to_zenlytic_measure,
)


@pytest.mark.metricflow
@pytest.mark.parametrize(
    "mf_dimension",
    [
        {"name": "no_sql", "type": "categorical", "label": "No SQL"},
        {"name": "basic_sql", "type": "categorical", "expr": "basic_sql_col"},
        {
            "name": "is_bulk_transaction",
            "type": "Categorical",
            "expr": "case when quantity > 10 then true else false end",
        },
        {
            "name": "deleted_at",
            "type": "time",
            "expr": "date_trunc('day', ts_deleted)",
            "is_partition": True,
            "type_params": {"time_granularity": "day"},
            "meta": {"zenlytic": {"zoe_description": "Deleted at field"}},
        },
    ],
)
def test_dimension_conversion(mf_dimension):
    converted = convert_mf_dimension_to_zenlytic_dimension(mf_dimension)

    if mf_dimension["name"] == "is_bulk_transaction":
        correct = {
            "name": "is_bulk_transaction",
            "field_type": "dimension",
            "type": "string",
            "sql": "case when quantity > 10 then true else false end",
            "searchable": True,
        }
    elif mf_dimension["name"] == "deleted_at":
        correct = {
            "name": "deleted_at",
            "field_type": "dimension_group",
            "type": "time",
            "sql": "date_trunc('day', ts_deleted)",
            "timeframes": ["raw", "date", "week", "month", "quarter", "year", "month_of_year"],
            "zoe_description": "Deleted at field",
        }

    elif mf_dimension["name"] == "no_sql":
        correct = {
            "name": "no_sql",
            "label": "No SQL",
            "field_type": "dimension",
            "type": "string",
            "sql": "${TABLE}.no_sql",
            "searchable": True,
        }
    elif mf_dimension["name"] == "basic_sql":
        correct = {
            "name": "basic_sql",
            "field_type": "dimension",
            "type": "string",
            "sql": "${TABLE}.basic_sql_col",
            "searchable": True,
        }

    assert converted == correct


@pytest.mark.metricflow
@pytest.mark.parametrize(
    "mf_measure",
    [
        {
            "name": "total_revenue",
            "description": "all gross revenue",
            "agg": "sum",
            "expr": "revenue",
            "agg_time_dimension": "created_at",
            "label": "Total Revenue (USD)",
        },
        {
            "name": "aov",
            "agg": "average",
            "expr": "case when email not ilike '%internal.com' then revenue end",
        },
        {
            "name": "quick_buy_transactions",
            "agg": "sum_boolean",
            "config": {"meta": {"zenlytic": {"zoe_description": "Quick buy transactions"}}},
        },
        {"name": "last_purchase", "agg": "max", "expr": "purchase_date", "create_metric": True},
        {
            "name": "p75_order_total",
            "expr": "order_total",
            "agg": "percentile",
            "agg_params": {"percentile": 0.75, "use_discrete_percentile": False},
            "create_metric": True,
        },
        {
            "name": "p99_order_total",
            "expr": "order_total",
            "agg": "percentile",
            "agg_params": {"percentile": 0.99, "use_discrete_percentile": True},
            "create_metric": True,
        },
        {
            "name": "mrr",
            "agg": "sum",
            "expr": "sub_rev",
            "create_metric": True,
            "non_additive_dimension": {
                "name": "order_date",
                "window_choice": "max",
                "window_groupings": ["customer_id"],
            },
        },
    ],
)
def test_measure_conversion(mf_measure):
    try:
        converted = convert_mf_measure_to_zenlytic_measure(mf_measure)
    except ZenlyticUnsupportedError as e:
        if "discrete percentile is not supported for the measure" in str(e):
            converted = {}
        else:
            raise e

    if mf_measure["name"] == "total_revenue":
        correct = {
            "name": "_total_revenue",
            "field_type": "measure",
            "sql": "revenue",
            "type": "sum",
            "label": "Total Revenue (USD)",
            "canon_date": "created_at",
            "description": "all gross revenue",
            "hidden": True,
        }
    elif mf_measure["name"] == "aov":
        correct = {
            "name": "_aov",
            "field_type": "measure",
            "sql": "case when email not ilike '%internal.com' then revenue end",
            "type": "average",
            "hidden": True,
        }
    elif mf_measure["name"] == "quick_buy_transactions":
        correct = {
            "name": "_quick_buy_transactions",
            "field_type": "measure",
            "sql": "CAST(quick_buy_transactions AS INT)",
            "type": "sum",
            "hidden": True,
            "zoe_description": "Quick buy transactions",
        }
    elif mf_measure["name"] == "last_purchase":
        correct = {"name": "last_purchase", "field_type": "measure", "sql": "purchase_date", "type": "max"}
    elif mf_measure["name"] == "mrr":
        correct = {
            "name": "mrr",
            "field_type": "measure",
            "sql": "sub_rev",
            "type": "sum",
            "non_additive_dimension": {
                "name": "order_date_raw",
                "window_choice": "max",
                "window_groupings": ["customer_id"],
            },
        }
    elif mf_measure["name"] == "p75_order_total":
        correct = {
            "name": "p75_order_total",
            "field_type": "measure",
            "sql": "order_total",
            "type": "percentile",
            "percentile": 75,
        }
    elif mf_measure["name"] == "p99_order_total":
        correct = {}

    assert converted == correct


@pytest.mark.metricflow
@pytest.mark.parametrize(
    "mf_metric",
    [
        {
            "name": "cumulative_metric",
            "description": "The metric description",
            "type": "cumulative",
            "label": "C Metric",
            "type_params": {
                "measure": {"name": "total_revenue"},
                "cumulative_type_params": {"window": "7 days"},
            },  # NOTE: we do not support, window or grain_to_date
        },
        {
            "name": "customers",
            "description": "Count of customers",
            "type": "simple",
            "label": "Count of customers",
            "type_params": {"measure": {"name": "customers"}},
        },
        {
            "name": "customers_old_syntax",
            "description": "Count of customers",
            "type": "simple",
            "label": "Count of customers",
            "type_params": {"measure": "customers"},
        },
        {
            "name": "large_orders",
            "description": "Order with order values over 20.",
            "type": "SIMPLE",
            "label": "Large Orders",
            "type_params": {"measure": {"name": "orders"}},
            "config": {
                "meta": {"zenlytic": {"zoe_description": "Order with order values over 20."}},
                "enabled": False,
            },
            "filter": "{{Dimension('order_id__order_total_dim')}} >= 20",
        },
        {
            "name": "many_filters_fail",
            "description": "Unique count of customers with many filters",
            "type": "SIMPLE",
            "label": "Many Filters",
            "type_params": {"measure": {"name": "customers"}},
            "filter": (
                "{{ Metric('food_revenue', group_by=['order_id']) }} > 0 "
                "and {{ Entity('product') }} in ('P3150104', 'P3150105') "
                "and {{ Dimension('customer__customer_type') }} = 'new' "
                "and ( {{ TimeDimension('customer__first_ordered_at', 'month') }} = '2024-01-01' "
                "or  {{ TimeDimension('customer__first_ordered_at', 'month') }} = '2024-02-01' "
                "or {{ TimeDimension('customer__first_ordered_at', 'day') }} is null)"
            ),
        },
        {
            "name": "many_filters",
            "description": "Unique count of customers with many filters",
            "type": "SIMPLE",
            "label": "Many Filters",
            "type_params": {"measure": {"name": "customers"}},
            "filter": (
                "{{ Dimension('customer__customer_type') }} = 'new' "
                "and ( {{ TimeDimension('customer__first_ordered_at', 'month') }} = '2024-01-01' "
                "or  {{ TimeDimension('customer__first_ordered_at', 'month') }} = '2024-02-01' "
                "or {{ TimeDimension('customer__first_ordered_at', 'day') }} is null)"
            ),
        },
        {
            "name": "food_order_pct",
            "description": "The food order count as a ratio of the total order count",
            "label": "Food Order Ratio",
            "type": "ratio",
            "type_params": {"numerator": {"name": "food_orders"}, "denominator": "orders"},
        },
        {
            "name": "frequent_purchaser_ratio",
            "description": "Fraction of active users who qualify as frequent purchasers",
            "owners": ["support@getdbt.com"],
            "type": "ratio",
            "type_params": {
                "numerator": {
                    "name": "distinct_purchasers",
                    "filter": "{{Dimension('customer__is_frequent_purchaser')}}",
                    "alias": "frequent_purchasers",
                },
                "denominator": {"name": "distinct_purchasers"},
            },
        },
        {
            "name": "order_gross_profit",
            "description": "Gross profit from each order.",
            "type": "derived",
            "label": "Order Gross Profit",
            "type_params": {
                "expr": "revenue - cost",
                "metrics": [
                    {"name": "order_total", "alias": "revenue"},
                    {"name": "order_cost", "alias": "cost"},
                ],
            },
        },
        {
            "name": "pct_rev_from_ads",
            "description": "Percentage of revenue from advertising.",
            "type": "derived",
            "label": "Percentage of Revenue from Advertising",
            "type_params": {
                "expr": "ad_total_revenue / total_revenue",
                "metrics": [
                    {"name": "ad_total_revenue"},
                    {"name": "total_revenue", "alias": "total_revenue"},
                ],
            },
        },
        {
            "name": "food_order_gross_profit",
            "label": "Food Order Gross Profit",
            "description": "The gross profit for each food order.",
            "type": "derived",
            "type_params": {
                "expr": "revenue - cost",
                "metrics": [
                    {
                        "name": "order_total",
                        "alias": "revenue",
                        "filter": "{{ Dimension('order_id__is_food_order') }} = True",
                    },
                    {
                        "name": "order_cost",
                        "alias": "cost",
                        "filter": "{{ Dimension('order_id__is_food_order') }} = True",
                    },
                ],
            },
        },
    ],
)
def test_metric_conversion(mf_metric):
    measures = [
        {"name": "customers", "agg": "count_distinct", "expr": "id_customer"},
        {"name": "orders", "agg": "count_distinct", "expr": "id_order"},
        {"name": "food_orders", "agg": "count_distinct", "expr": "case when is_food then id_order end"},
        {"name": "distinct_purchasers", "agg": "count_distinct", "expr": "id_customer"},
        {"name": "order_total", "agg": "sum", "expr": "num_order_total"},
        {"name": "order_cost", "agg": "sum", "expr": "num_order_cost"},
    ]
    primary_key_mapping = {"order_id": "orders", "customer": "customers"}
    try:
        converted, _ = convert_mf_metric_to_zenlytic_measure(mf_metric, measures, primary_key_mapping)
    except ZenlyticUnsupportedError as e:
        if "Entity type filters are not supported" in str(e) and mf_metric["name"] == "many_filters_fail":
            converted = {}
        elif "It is a cumulative metric," in str(e):
            converted = {}
        else:
            raise e

    if mf_metric["name"] == "cumulative_metric":
        correct = {}
    elif mf_metric["name"] == "pct_rev_from_ads":
        correct = {
            "name": "pct_rev_from_ads",
            "field_type": "measure",
            "sql": "${ad_total_revenue} / ${total_revenue}",
            "type": "number",
            "label": "Percentage of Revenue from Advertising",
            "description": "Percentage of revenue from advertising.",
        }
    elif mf_metric["name"] == "customers":
        correct = {
            "name": "customers",
            "field_type": "measure",
            "sql": "id_customer",
            "hidden": False,
            "type": "count_distinct",
            "label": "Count of customers",
            "description": "Count of customers",
        }
    elif mf_metric["name"] == "customers_old_syntax":
        correct = {
            "name": "customers_old_syntax",
            "field_type": "measure",
            "sql": "id_customer",
            "hidden": False,
            "type": "count_distinct",
            "label": "Count of customers",
            "description": "Count of customers",
        }
    elif mf_metric["name"] == "large_orders":
        # Push down the filter to a new filtered measure
        correct = {
            "name": "large_orders",
            "field_type": "measure",
            "hidden": True,
            "sql": "case when ${orders.order_total_dim} >= 20 then id_order else null end",
            "type": "count_distinct",
            "label": "Large Orders",
            "description": "Order with order values over 20.",
            "zoe_description": "Order with order values over 20.",
        }
    elif mf_metric["name"] == "many_filters_fail":
        correct = {}
    elif mf_metric["name"] == "many_filters":
        correct = {
            "name": "many_filters",
            "field_type": "measure",
            "hidden": False,
            "sql": (
                "case when  ${customers.customer_type}  = 'new' and (  "
                "${customers.first_ordered_at_month}  = '2024-01-01' or   "
                "${customers.first_ordered_at_month}  = '2024-02-01' or  "
                "${customers.first_ordered_at_date}  is null) then id_customer else "
                "null end"
            ),
            "type": "count_distinct",
            "label": "Many Filters",
            "description": "Unique count of customers with many filters",
        }
    elif mf_metric["name"] == "food_order_pct":
        correct = {
            "name": "food_order_pct",
            "field_type": "measure",
            "sql": "${food_orders} / ${orders}",
            "type": "number",
            "label": "Food Order Ratio",
            "description": "The food order count as a ratio of the total order count",
        }
    elif mf_metric["name"] == "frequent_purchaser_ratio":
        correct = {
            "name": "frequent_purchaser_ratio",
            "field_type": "measure",
            "sql": "${frequent_purchaser_ratio_numerator} / ${distinct_purchasers}",
            "type": "number",
            "label": "Frequent Purchaser Ratio",
            "description": "Fraction of active users who qualify as frequent purchasers",
        }
    elif mf_metric["name"] == "order_gross_profit":
        correct = {
            "name": "order_gross_profit",
            "field_type": "measure",
            "sql": "${order_total} - ${order_cost}",
            "type": "number",
            "label": "Order Gross Profit",
            "description": "Gross profit from each order.",
        }
    elif mf_metric["name"] == "food_order_gross_profit":
        correct = {
            "name": "food_order_gross_profit",
            "field_type": "measure",
            "sql": "${food_order_gross_profit_revenue} - ${food_order_gross_profit_cost}",
            "type": "number",
            "label": "Food Order Gross Profit",
            "description": "The gross profit for each food order.",
        }

    assert converted == correct


@pytest.mark.metricflow
@pytest.mark.parametrize(
    "mf_entity",
    [
        {"name": "transaction", "type": "primary", "expr": "id_transaction"},
        {"name": "order", "type": "foreign", "expr": "id_order"},
        {"name": "order_line_id", "type": "unique", "expr": "id_order_line"},
        {"name": "id_order_line", "type": "unique"},
        {"name": "id_order_line_2", "type": "natural"},
        {
            "name": "order_line_unique_id",
            "type": "unique",
            "config": {
                "meta": {
                    "zenlytic": {"sql": "CAST(id_order_line AS STRING)", "allow_fanouts": ["my_other_view"]}
                }
            },
        },
    ],
)
def test_entity_conversion(mf_entity):
    if mf_entity["name"] in {"order_line_id", "id_order_line"}:
        fields = [{"name": "id_order_line", "sql": "CAST(order_line_unique_id AS STRING)"}]
    else:
        fields = []

    converted = convert_mf_entity_to_zenlytic_identifier(mf_entity, fields=fields)

    if mf_entity["name"] == "transaction":
        correct = {"name": "transaction", "type": "primary", "sql": "${TABLE}.id_transaction"}
    elif mf_entity["name"] == "order":
        correct = {"name": "order", "type": "foreign", "sql": "${TABLE}.id_order"}
    elif mf_entity["name"] == "order_line_id":
        correct = {"name": "order_line_id", "type": "primary", "sql": "${id_order_line}"}
    elif mf_entity["name"] == "id_order_line":
        correct = {"name": "id_order_line", "type": "primary", "sql": "${id_order_line}"}
    elif mf_entity["name"] == "id_order_line_2":
        correct = {"name": "id_order_line_2", "type": "primary", "sql": "${TABLE}.id_order_line_2"}
    elif mf_entity["name"] == "order_line_unique_id":
        correct = {
            "name": "order_line_unique_id",
            "type": "primary",
            "sql": "CAST(id_order_line AS STRING)",
            "allow_fanouts": ["my_other_view"],
        }
    assert converted == correct
