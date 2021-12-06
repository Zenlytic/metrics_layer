---
sidebar_position: 1
---

# Exploring the Data model

There are several options in Metrics Layer for exploring a data model. Here are some examples of their usage:


### Explores

When listing explores, the default is to return a list of `Explore` [objects](../6_project/3_explore.md). If you're not very familiar with the concept of an explore, it is essentially a grouping of tables that can be joined together. More information is available in Looker's [docs](https://docs.looker.com/reference/explore-params/explore).

```
from metrics_layer import MetricsLayerConnection

conn = MetricsLayerConnection("demo_connection")

# Lists of *all* the explores in your data model
explores = conn.list_explores()

# You can also get a single explore based on it's name.
explore = conn.get_explore("order_lines")
```


### Metrics

When listing metrics, the default is to return a list of `Field` [objects](../6_project/5_field.md). Listing metrics will return all measures associated with your LookML project.

```
from metrics_layer import MetricsLayerConnection

conn = MetricsLayerConnection("demo_connection")

# Lists of *all* the metrics in your data model
metrics = conn.list_metrics()

# List of metrics in this explore
metrics_in_orders = conn.list_metrics(explore_name="orders")

# List of metrics in this explore, specifically in this view
metrics_in_orders_customers_view = conn.list_metrics(explore_name="orders", view_name="customers")

# You can also get a single metric based on it's name.
# The below three calls return the same thing

# Metric name
metric = conn.get_metric("total_revenue")

# View and metric name
metric = conn.get_metric("orders.total_revenue")

# Explore, view and metric name
metric = conn.get_metric("order_lines.orders.total_revenue")
```


### Dimensions

When listing dimensions, like listing metrics, the default is to return a list of `Field` [objects/](../6_project/5_field.md). Listing dimensions will return all dimensions and dimension_groups associated with your LookML project.

```
# Lists of *all* the dimensions in your data model
dimensions = conn.list_dimensions()

# List of dimensions in this explore
dimensions_in_orders = conn.list_dimensions(explore_name="orders")

# List of dimensions in this explore, specifically in this view
dimensions_in_orders_customers_view = conn.list_dimensions(explore_name="orders", view_name="customers")

# You can also get a single dimension based on it's name.
# The below three calls return the same thing

# Dimension name
dimension = conn.get_dimension("total_revenue")

# View and dimension name
dimension = conn.get_dimension("orders.total_revenue")

# Explore, view and dimension name
dimension = conn.get_dimension("order_lines.orders.total_revenue")
```