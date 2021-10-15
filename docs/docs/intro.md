---
sidebar_position: 1
---

# What is Granite?

Granite is an open source metrics layer. It's goal is to make access of metrics consistent throughout an organization. We believe you should be able to access consistent metrics from any tool you use to access data.

## How does it work?

Right now, the only supported BI tool is Looker. Granite will read your LookML and give you the ability to access those metrics and dimensions in a python client library, or through SQL with a special `MQL` tag.

The python client library looks like this:


```
# References a profile defining where to find the LookML and how to connect to the data warehouse
conn = GraniteConnection("demo")

# Generates the SQL query and runs it against the warehouse, returns a pandas dataframe
df = conn.query(
    metrics=["total_revenue"],
    dimensions=["order_month", "acquisition_channel"]
)
```


The SQL syntax looks like this:

```
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


These queries reference the measures and dimensions by name exactly like you would in the Looker interface, but give you a greater ability to compose them with additional data and access them in more technical tools.

## What is the workflow like?

Since Granite references existing resources, it's easy to set up.

1. **[Install granite](./getting_started.md#installation)**
2. **[Set up a profile](./getting_started.md#profile-set-up)** to connect to your data model and warehouse
3. Execute commands in python


## Who should use Granite?

Granite is appropriate for anyone who wants to query consistent metrics defined in LookML using more technical tools like SQL or python.

To make full use of Granite, it's helpful to be comfortable in either SQL or python.
