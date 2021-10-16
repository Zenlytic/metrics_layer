---
sidebar_position: 2
---

# Getting Started

Set granite up and start querying your metrics in **in under 2 minutes**.

## Installation

Make sure that your data warehouse is one of the supported types. Granite currently supports Snowflake and BigQuery, and only works with `python >= 3.7`.

Install granite with the appropriate extra for your warehouse

For Snowflake run `pip install granite-metrics[snowflake]`

For BigQuery run `pip install granite-metrics[bigquery]`


## Profile set up

There are several ways to set up a profile, we're going to look at the fastest one here, but look at [other connection options](./3_connection_setup/connecting.md) if you want more robust connection methods.

The fastest way to get connected is to pass the necessary information directly into Granite. Once you've installed the library with the warehouse you need, you should be able to run the code snippet below and start querying.

You'll need to pull the repo with your LookML locally for this example or look at [other connection options](./3_connection_setup/connecting.md) for connections through GitHub directly or the Looker API.


```
from granite import GraniteConnection

# Give granite the info to connect to your data model and warehouse
config = {
  "repo_path": "~/Desktop/my-looker-repo",
  "connections": [
    "name": "mycompany",              # The name of the connection in LookML (you'll see this in model files)
    "type": "snowflake",
    "account": "2e12ewdq.us-east-1",
    "username": "demo_user",
    "password": "q23e13erfwefqw",
    "database": "ANALYTICS",          # Optional
    "schema": "DEV",                  # Optional
  ],
}
conn = GraniteConnection(config)

# You're off to the races. Query away!
df = conn.query(metrics=["total_revenue"], dimensions=["channel", "region"])
```

That's it.

For more advanced methods of connection check out [other connection options](./3_connection_setup/connecting.md) and for supported data warehouses look at [supported warehouses](./3_connection_setup/integrations.md).

