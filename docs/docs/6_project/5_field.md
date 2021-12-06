---
sidebar_position: 5
---

# Field

The field object has an important function `alias()` that will format the group using it's `name` and `dimension_group` (e.g. `order_date`) like you will see in the resulting dataframe.

It also has a `field.sql` property that is the raw sql (without substitution) that Metrics Layer derives for the field, if it's not already specified.

Properties of fields, like other objects, can be accessed like `field.description`.
