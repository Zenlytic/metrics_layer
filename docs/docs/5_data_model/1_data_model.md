---
sidebar_position: 1
---

# Data Model Intro

There are two supported types of data models LookML and Metrics Layer's own yaml format. We'll go over that format and how to create a data model using this yaml format.


### Modeling concepts

There are a few concepts that are helpful in data modeling

1. The Project

The project is pretty simple. It's just the collection of all the models, explores, views and fields.

2. Model

The model's purpose is to connect data warehouses with explores. A model has a connection to a data warehouse and serves as a container for explores, so the metrics layer knows what data warehouse to use when querying data in that explore.

3. Explore

The explore is a group of views that can be (optionally) joined together from a set of rules. Explores have a base view (similar to a table), and then rules in yaml for specifying the nature of the join relationship and how to execute the join. They do exciting things like allow you to connect one to many tables and see correctly calculated metrics by otherwise difficult relationships.

4. View

The view represents a table. It serves as a translation from a table with rows and columns to a set of metrics (or measures) amd dimensions. More on that distinction below.

5. Field (metric / measure or dimension)

Fields are either metrics (a.k.a measures), dimensions, or dimension_groups

Metrics are aggregated columns. Conceptually, they are how you map the idea "Number of orders" to the sql statement `count(distinct order_id)` repeatedly and correctly, even when complicated joins happen inside the explore.

Dimensions are columns in the database. You can reference these in metrics, in each other and use them to `group by` the table that the view is referencing.

Dimension Groups also reference columns in the database, but they have a grouping of methods you would apply to the same column. For example, the most common dimension group is a date column with timeframes like `[date, week, month, year]` each group in that list corresponds to a `date_trunc` operation on the date column referenced. It helps to keep those consistent and organized for later reference.


### Development

There are a few cli methods Metrics Layer provides using its cli `ml`.

The first is the `debug` command. Once you finish [installation](../getting_started.md#installation) and [connection setup](../3_connection_setup/connecting.md), simply run the following command from the terminal.

```
$ ml debug my_profile_name
```

One of the most helpful if you're just starting out is the `seed` method. If you haven't started writing your data model or if you've recently added a new database, it's a pain to go in and manually set up the explores, views and columns. `seed` allows you to specify a database and optionally a schema, and it will programmatically read the tables in that database or schema and generate the explores, views, and fields  to give you a nice starting place.

Here are some examples (the `$` indicates these examples are taking place in the terminal):

If you want to generate initial views from only tables in the `prod_zendesk` schema inside the `analytics` database

```
$ ml seed --database analytics --schema prod_zendesk my_profile_name
```


If you want to generate initial views from all tables inside the `analytics` database

```
$ ml seed --database analytics my_profile_name
```

Another helpful feature is the `validate` feature. It allows you to statically check if you have any broken references or invalid types in your project before deploying it anywhere or using it top query live data. It is run with the following command.

```
$ ml validate my_profile_name
```

Two additional helper commands are the `list` and `show` commands. The `list` command lists the names of objects from the type you pass, and the `show` command shows printable attributes of the object with the name you pass. Here are some examples.

This lists the names of all the explores in your project

```
$ ml list --profile my_profile_name explores
```

This command lists all the views in the explore `transaction_lines`

```
$ ml list --profile my_profile_name --explore transaction_lines views
```

This command lists all the metrics (fields of type `measure`) in the view `transactions`

```
$ ml list --profile my_profile_name --view transactions metrics
```

This command shows the attributes of the field `total_revenue` in the explore `transaction_lines`

```
$ ml show --profile my_profile_name --explore transaction_lines --type field total_revenue
```

This command shows the attributes of the explore `transaction_lines`

```
$ ml show --profile my_profile_name --type explore transaction_lines
```

Have any other ideas for good cli tools to help with development? Open a [GitHub issue](https://github.com/Zenlytic/metrics_layer/issues) and let us know!