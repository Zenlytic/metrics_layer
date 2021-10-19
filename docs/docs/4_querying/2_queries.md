---
sidebar_position: 2
---

# Getting SQL and running queries

There are two main methods for interacting with SQL in Metrics Layer `get_sql_query`, which gets the SQL necessary to calculate your request, but doesn't run it, and `query`, which gets that SQL and runs it against your warehouse.

In both of these methods there are two ways to use Metrics Layer, using SQL with a `MQL` tag for metrics, or specifying lists of metrics and dimensions.

:::tip Query speed

In all cases, Metrics Layer generates the SQL query locally, then sends it directly to your warehouse. This is an order of magnitude faster than using the Looker API or similar.

:::

## Metrics and dimensions

Here's an example of specifying metrics and dimensions to query:

```
from metrics_layer import MetricsLayerConnection

conn = MetricsLayerConnection("demo")

# Generates the SQL query and returns it as a string
sql_query = conn.get_sql_query(
    metrics=["total_revenue"],
    dimensions=["order_month", "acquisition_channel"]
)

# Generates the SQL query and runs it against the warehouse, returns a pandas dataframe
df = conn.query(
    metrics=["total_revenue"],
    dimensions=["order_month", "acquisition_channel"]
)
```

## MQL queries

Here's an example of using the `MQL` syntax to compose queries to run against the warehouse. You can include queries with only `MQL`, queries that compose `MQL` with other SQL, or queries that are only SQL (in this case you'll have to pass a `connection_name` argument because Metrics Layer will not be able to determine which connection to use).


### MQL only
```
# Example using MQL only
query = "MQL(total_revenue BY acquisition_channel)"

# Returns a string
raw_sql_query = conn.get_sql_query(sql=query)

# Returns a pandas dataframe
df = conn.query(sql=query)
```


### MQL and SQL
```
# Example composing MQL and SQL
query = """
    SELECT
        channel_details.channel_name,
        channel_details.channel_owner,
        channel_revenue.total_revenue
    FROM MQL(
            total_revenue
            BY
            acquisition_channel
        ) as channel_revenue
        LEFT JOIN analytics.channel_details as channel_details
            ON channel_revenue.acquisition_channel = channel_details.channel_name
"""

df = conn.query(sql=query)
```

### SQL only
```
# Example with SQL only
query = """
    SELECT
        channel_details.channel_name,
        channel_details.channel_owner
    FROM analytics.channel_details as channel_details
"""

df = conn.query(sql=query, connection_name="mycompany_snowflake")
```

