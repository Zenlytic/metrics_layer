---
sidebar_position: 1
---

# Project

The project object is the class you use to interact with your project as a whole. It has many convenience methods that allow you to look around the project.

Once you create a connection, you can access the project like this:

```
from metrics_layer import MetricsLayerConnection

conn = MetricsLayerConnection("demo")

project = conn.project
```

Using the project object you can look around your entire data model


### Models
```
# Models
all_models_in_project = project.models()

a_single_model = project.get_model(model_name="revenue")
```


### Explores
```
# Explores
all_explores_in_project = project.explores()

a_single_explore = project.get_explore(explore_name="order_lines")
```


### Views
```
# Views
all_views_in_project = project.views()

all_views_in_explore = project.views(explore_name="order_lines")

a_single_view = project.get_view(view_name="orders")
```


### Fields
```
# Fields
all_fields_in_project = project.fields()

all_fields_in_explore = project.fields(explore_name="order_lines")

all_fields_in_view_in_explore = project.fields(explore_name="order_lines", view_name="orders")

# Only specify field name
a_single_field = project.get_field("total_revenue")

# Specify view and field name
a_single_field = project.get_field("orders.total_revenue")

# Specify explore, view and field name
a_single_field = project.get_field("order_lines.orders.total_revenue")

# OR pass them as arguments
a_single_field = project.get_field("total_revenue", explore_name="order_lines", view_name="orders")
```


To learn more about any of the objects returned here, look at the documentation for [models](./2_model.md), [explores](./3_explore.md), [views](./4_view.md), or [fields](./5_field.md).