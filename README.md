# Metrics Layer

[![Build Status](https://app.travis-ci.com/Zenlytic/metrics_layer.svg?branch=master)](https://app.travis-ci.com/Zenlytic/metrics_layer)
[![codecov](https://codecov.io/gh/Zenlytic/metrics_layer/branch/master/graph/badge.svg?token=7JA6PKNV57)](https://codecov.io/gh/Zenlytic/metrics_layer)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# What is a Metrics Layer?

Metrics Layer is an open source project with the goal of making access to metrics consistent throughout an organization. We believe you should be able to access consistent metrics from any tool you use to access data. This metrics layer is designed to work with [Zenlytic](https://zenlytic.com) as a BI tool. 

## How does it work?

Right now, [Zenlytic](https://zenlytic.com) is the only supported BI tool. The Metrics Layer will read your data model and give you the ability to access those metrics and dimensions in a python client library, or through SQL with a special `MQL` tag.

Sound interesting? Here's how to set Metrics Layer up with your data model and start querying your metrics in **in under 2 minutes**.

## Installation

Make sure that your data warehouse is one of the supported types. Metrics Layer currently supports Snowflake, BigQuery and Redshift, and only works with `python >= 3.8`.

Install Metrics Layer with the appropriate extra for your warehouse

For Snowflake run `pip install metrics-layer[snowflake]`

For BigQuery run `pip install metrics-layer[bigquery]`

For Redshift run `pip install metrics-layer[redshift]`


## Profile set up

There are several ways to set up a profile, we're going to look at the fastest one here.

The fastest way to get connected is to pass the necessary information directly into Metrics Layer. Once you've installed the library with the warehouse you need, you should be able to run the code snippet below and start querying.

You'll pull the repo from Github for this example. For more detail on getting set up, check out the [documentation](https://docs.zenlytic.com)!


```
from metrics_layer import MetricsLayerConnection

# Give metrics_layer the info to connect to your data model and warehouse
config = {
  "location": "https://myusername:myaccesstoken@github.com/myorg/myrepo.git",
  "branch": "develop",
  "connections": [
    {
      "name": "mycompany",              # The name of the connection in your data model (you'll see this in model files)
      "type": "snowflake",
      "account": "2e12ewdq.us-east-1",
      "username": "demo_user",
      "password": "q23e13erfwefqw",
      "database": "ANALYTICS",
      "schema": "DEV",                  # Optional
    }
  ],
}
conn = MetricsLayerConnection(**config)

# You're off to the races. Query away!
df = conn.query(metrics=["total_revenue"], dimensions=["channel", "region"])
```

That's it.

For more advanced methods of connection and more information about the project check out [the docs](https://docs.zenlytic.com).
