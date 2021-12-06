---
sidebar_position: 2
---

# Getting Started

Set Metrics Layer up and start querying your metrics in **in under 2 minutes**.

## Installation

Make sure that your data warehouse is one of the supported types. Metrics Layer currently supports Snowflake and BigQuery, and only works with `python >= 3.7`.

Install Metrics Layer with the appropriate extra for your warehouse

For Snowflake run `pip install metrics-layer[snowflake]`

For BigQuery run `pip install metrics-layer[bigquery]`


## Profile set up

There are several ways to set up a profile, we're going to look at the fastest one here, but look at [other connection options](./3_connection_setup/connecting.md) if you want more robust connection methods.

The fastest way to get connected is to pass the necessary information directly into Metrics Layer. Once you've installed the library with the warehouse you need, you should be able to run the code snippet below and start querying.

You'll need to pull the repo with your LookML or [metrics layer data model](./5_data_model/1_data_model.md) locally for this example or look at [other connection options](./3_connection_setup/connecting.md) for connections through GitHub directly or the Looker API.


```
from metrics_layer import MetricsLayerConnection

# Give metrics_layer the info to connect to your data model and warehouse
config = {
  "repo_path": "~/Desktop/my-lookml-repo",
  "connections": [
    {
      "name": "mycompany",              # The name of the connection in LookML or yaml (you'll see this in model files)
      "type": "snowflake",
      "account": "2e12ewdq.us-east-1",
      "username": "demo_user",
      "password": "q23e13erfwefqw",
      "database": "ANALYTICS",          # Optional
      "schema": "DEV",                  # Optional
    }
  ],
}
conn = MetricsLayerConnection(config)

# You're off to the races. Query away!
df = conn.query(metrics=["total_revenue"], dimensions=["channel", "region"])
```

That's it.

For more advanced methods of connection check out [other connection options](./3_connection_setup/connecting.md) and for supported data warehouses look at [supported warehouses](./3_connection_setup/integrations.md).

