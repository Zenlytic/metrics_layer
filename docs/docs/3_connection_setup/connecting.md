---
sidebar_position: 1
---

# Connecting

First, we'll go through how to set up a `profiles.yml` file, which is the best solution for an individual using Metrics Layer on his or her local machine. Second, we'll look at other ways of passing the configuration into Metrics Layer.

## Profile set up

There are three ways to set up a profile, that is access the data model and find credentials for your data warehouse:

1. Local repo
2. Github repo
3. Looker API

Metrics Layer gets this information by looking for a `profiles.yml` file (similar to [dbt](https://www.getdbt.com)) in the `~/.metrics_layer/` directory by default. You can change this directory by specifying the `METRICS_LAYER_PROFILES_DIR` environment variable. Now let's look at examples of each type

### Local repo

This is the best method when the repo with your LookML or [metrics layer data model](../5_data_model/1_data_model.md) is on your local machine. Your `profiles.yml` will looks like this with a connection to Snowflake.

```
demo_connection:
  target: dev
  outputs:
    dev:
      repo_path: ~/Desktop/my_company_lookml/
      connections:
        - name: my_company                  # This references the connection string in the LookML or yaml model argument 'connection'
          type: snowflake
          account: 123p0iwe.us-east-1
          username: demo_user
          password: very_secure_password
          warehouse: compute_wh             # optional
          database: demo                    # optional
          schema: analytics                 # optional

```

You will be able to connect with the following python code.

```
from metrics_layer import MetricsLayerConnection

conn = MetricsLayerConnection("demo_connection")

df = conn.query(metrics=["total_revenue"], dimensions=["channel", "region"])
```

### Github repo

This is the best method when the repo with your LookML or [metrics layer data model](../5_data_model/1_data_model.md) is in a GitHub repo you have access to. Your `profiles.yml` will looks like this with a connection to BigQuery. We've also added multiple targets. If you need help creating a GitHub personal access token, check out [their docs](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token).

```
demo_connection:
  target: dev
  outputs:
    dev:
      repo_url: https://{YOUR_GITHUB_USERNAME}:{YOUR_GITHUB_ACCESS_TOKEN}@github.com/my_company/my_company_lookml
      branch: dev
      looker_env: dev           # This sets the Looker environment when reading your LookML or yaml
      connections:
        - name: my_company_bq   # This references the connection string in the LookML or yaml model argument 'connection'
          type: bigquery
          project: my-company-development
          credentials: ./my-company-dev-service-account-credentials.json
    prod:
      repo_url: https://{YOUR_GITHUB_USERNAME}:{YOUR_GITHUB_ACCESS_TOKEN}@github.com/my_company/my_company_lookml
      branch: master
      looker_env: prod
      connections:
        - name: my_company_bq
          type: bigquery
          project: my-company-prod
          credentials: ./my-company-prod-service-account-credentials.json
```

You will be able to connect with the following python code. You can explicitly specify the target you want to connect to, to tell Metrics Layer to use something besides the default.

```
from metrics_layer import MetricsLayerConnection

conn = MetricsLayerConnection("demo_connection", target="prod")

df = conn.query(metrics=["total_revenue"], dimensions=["channel", "region"])
```

### Looker API

This is the best method when the repo with your LookML is behind the Looker API. Your `profiles.yml` will looks like this with a connection to Snowflake.

```
demo_api_connection:
  target: dev
  outputs:
    dev:
      project_name: my_company                              # The name of your Looker Project
      looker_url: https://mycompany.cloud.looker.com        # The API endpoint for your Looker instance
      client_id: 13412e3qedqwdqe2e13e                       # The client ID from your Looker API3 credentials
      client_secret: 342r3e2erfw23e1241rewfwer12e1          # The client secret from your Looker API3 credentials
      connections:
        - name: my_company                  # This references the connection string in the LookML model argument 'connection'
          type: snowflake
          account: 123p0iwe.us-east-1
          username: demo_user
          password: very_secure_password
          warehouse: compute_wh             # optional
          database: demo                    # optional
          schema: analytics                 # optional

```

You will be able to connect with the following python code.

```
from metrics_layer import MetricsLayerConnection

conn = MetricsLayerConnection("demo_api_connection")

df = conn.query(metrics=["total_revenue"], dimensions=["channel", "region"])
```


## Other ways to connect

Using `profiles.yml` is a good solution for local work but doesn't work for all situations. These are the ways to connect (ranked in the order that Metrics Layer will respect them):

1. Explicitly pass a `dict` with the values in the `profiles.yml` file.
2. Use `profiles.yml`

The first one is the example used in the [getting started example](../getting_started.md), and supports the same syntax as the `profiles.yml` file.

