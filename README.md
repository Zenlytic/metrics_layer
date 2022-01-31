# Metrics Layer

[![Build Status](https://app.travis-ci.com/Zenlytic/metrics_layer.svg?branch=master)](https://app.travis-ci.com/Zenlytic/metrics_layer)
[![codecov](https://codecov.io/gh/Zenlytic/metrics_layer/branch/master/graph/badge.svg?token=7JA6PKNV57)](https://codecov.io/gh/Zenlytic/metrics_layer)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# What is Metrics Layer?

Metrics Layer is an open source project with the goal of making access to metrics consistent throughout an organization. We believe you should be able to access consistent metrics from any tool you use to access data.

## How does it work?

Right now, the only supported BI tool is Looker. Metrics Layer will read your LookML and give you the ability to access those metrics and dimensions in a python client library, or through SQL with a special `MQL` tag.

Sound interesting? Here's how to set Metrics Layer up with Looker and start querying your metrics in **in under 2 minutes**.

## Installation

Make sure that your data warehouse is one of the supported types. Metrics Layer currently supports Snowflake and BigQuery, and only works with `python >= 3.7`.

Install Metrics Layer with the appropriate extra for your warehouse

For Snowflake run `pip install metrics-layer[snowflake]`

For BigQuery run `pip install metrics-layer[bigquery]`


## Profile set up

There are several ways to set up a profile, we're going to look at the fastest one here, but look at [the docs](https://zenlytic.github.io/metrics_layer/docs/connection_setup/connecting) if you want more robust connection methods.

The fastest way to get connected is to pass the necessary information directly into Metrics Layer. Once you've installed the library with the warehouse you need, you should be able to run the code snippet below and start querying.

You'll need to pull the repo with your LookML locally for this example or look at [the docs](https://zenlytic.github.io/metrics_layer/docs/connection_setup/connecting) for connections through GitHub directly or the Looker API.


```
from metrics_layer import MetricsLayerConnection

# Give metrics_layer the info to connect to your data model and warehouse
config = {
  "repo_path": "~/Desktop/my-looker-repo",
  "connections": [
    {
      "name": "mycompany",              # The name of the connection in LookML (you'll see this in model files)
      "type": "snowflake",
      "account": "2e12ewdq.us-east-1",
      "username": "demo_user",
      "password": "q23e13erfwefqw",
      "database": "ANALYTICS",
      "schema": "DEV",                  # Optional
    }
  ],
}
conn = MetricsLayerConnection(config)

# You're off to the races. Query away!
df = conn.query(metrics=["total_revenue"], dimensions=["channel", "region"])
```

That's it.

For more advanced methods of connection and more information about the project check out [the docs](https://zenlytic.github.io/zenlytic-docs/).
