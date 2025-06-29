import os
import re
from glob import glob

import ruamel.yaml
import sqlglot

from .metricflow_types import MetricflowMetricTypes


class ZenlyticUnsupportedError(Exception):
    pass


def convert_mf_project_to_zenlytic_project(
    mf_project: dict,
    project_name: str = "mf_project_name",
    connection_name: str = "mf_connection_name",
    model_dict: dict = {},
) -> tuple:
    """mf_project is a dict with keys for each semantic model
    and the dims, measures, and metrics associated with it
    """
    all_measures = []
    for semantic_model in mf_project.values():
        all_measures.extend(semantic_model.get("measures", []))

    primary_key_mapping = get_primary_key_mapping(mf_project)
    model = {**model_dict, "version": 1, "type": "model", "name": project_name, "connection": connection_name}
    views, errors = [], []
    for _, semantic_model in mf_project.items():
        view, view_errors = convert_mf_view_to_zenlytic_view(
            semantic_model, model["name"], all_measures, primary_key_mapping
        )
        views.append(view)
        errors.extend(view_errors)

    return [model], views, errors


def get_primary_key_mapping(semantic_models: dict) -> dict:
    """
    Extract primary key identifiers from semantic models and create a mapping
    from primary key to view name.

    Args:
        semantic_models: Dictionary of semantic models with their configurations

    Returns:
        Dictionary mapping primary key names to view names
    """
    pk_mapping = {}

    for view_name, semantic_model in semantic_models.items():
        entities = semantic_model.get("entities", [])

        # Find the primary entity (primary key)
        for entity in entities:
            if entity.get("type") == "primary":
                pk_name = entity.get("name")
                if pk_name:
                    pk_mapping[pk_name] = view_name
                break

    return pk_mapping


def load_mf_project(models_folder: str):
    semantic_models, metrics = {}, []
    for fn in read_mf_project_files(models_folder):
        mf_model_dict = convert_yml_to_dict(fn)

        metrics.extend(mf_model_dict.get("metrics", []))
        for semantic_model in mf_model_dict.get("semantic_models", []):
            semantic_models[semantic_model["name"]] = semantic_model
            # Empty list of metrics to be filled below
            semantic_models[semantic_model["name"]]["metrics"] = []

    # Assign metrics to the view they should logically live in
    for metric in metrics:
        type_params = metric["type_params"]
        referenced_metrics = []

        if metric["type"] in {MetricflowMetricTypes.simple, MetricflowMetricTypes.cumulative}:
            referenced_metrics.append(get_name_or_string_literal(type_params["measure"]))
        elif metric["type"] == MetricflowMetricTypes.ratio:
            referenced_metrics.append(get_name_or_string_literal(type_params["numerator"]))
            referenced_metrics.append(get_name_or_string_literal(type_params["denominator"]))
        elif metric["type"] == MetricflowMetricTypes.derived:
            for ref_metric in type_params["metrics"]:
                referenced_metrics.append(ref_metric["name"])

        # First try to find a semantic model that has a measure matching any referenced metric/measure
        assigned = False
        for ref_item in referenced_metrics:
            if assigned:
                break

            for model_name, semantic_model in semantic_models.items():
                if assigned:
                    break

                # Check if the reference is a measure in this semantic model
                for measure in semantic_model.get("measures", []):
                    if ref_item == measure["name"]:
                        semantic_models[model_name]["metrics"].append(metric)
                        assigned = True
                        break

        # If not assigned yet, check if any referenced item is another metric already assigned to a model
        if not assigned:
            for ref_item in referenced_metrics:
                if assigned:
                    break

                for model_name, semantic_model in semantic_models.items():
                    if assigned:
                        break

                    # Check if the reference is a metric already assigned to this semantic model
                    for existing_metric in semantic_model.get("metrics", []):
                        if ref_item == existing_metric["name"]:
                            semantic_models[model_name]["metrics"].append(metric)
                            assigned = True
                            break

        if not assigned:
            print(f"WARNING: Could not assign metric {metric['name']} to any semantic model")

    return semantic_models


def convert_mf_view_to_zenlytic_view(
    mf_semantic_model: dict,
    model_name: str,
    all_measures: list,
    primary_key_mapping: dict,
    original_file_path: str = None,
) -> tuple:
    def _error_func(error, extra: dict = {}):
        return {**extra, "view_name": mf_semantic_model["name"], "message": error}

    errors = []
    zenlytic_data = {"version": 1, "type": "view", "model_name": model_name, "fields": [], "identifiers": []}

    mf_metrics = mf_semantic_model.get("metrics", [])

    if original_file_path:
        zenlytic_data["original_file_path"] = original_file_path

    # Get view-level values
    zenlytic_data["name"] = mf_semantic_model["name"]
    if "sql_table_name" in mf_semantic_model.get("meta", {}):
        zenlytic_data["sql_table_name"] = mf_semantic_model["meta"]["sql_table_name"]
    else:
        zenlytic_data["sql_table_name"] = extract_inner_text(mf_semantic_model["model"])

    if mf_semantic_model.get("config", {}).get("meta") and isinstance(
        mf_semantic_model["config"]["meta"], dict
    ):
        zenlytic_data = {**zenlytic_data, **mf_semantic_model["config"]["meta"].get("zenlytic", {})}

    if description := mf_semantic_model.get("description"):
        zenlytic_data["description"] = description
    default_date = mf_semantic_model.get("defaults", {}).get("agg_time_dimension")

    if default_date:
        zenlytic_data["default_date"] = default_date

    # dimensions to fields.dimensions
    for dimension in mf_semantic_model.get("dimensions", []):
        try:
            field_dict = convert_mf_dimension_to_zenlytic_dimension(dimension)
            zenlytic_data["fields"].append(field_dict)
        except ZenlyticUnsupportedError as e:
            errors.append(_error_func(f"In view {mf_semantic_model['name']} {str(e)}"))

    # measures to measures
    for measure in mf_semantic_model.get("measures", []):
        try:
            field_dict = convert_mf_measure_to_zenlytic_measure(measure)
            zenlytic_data["fields"].append(field_dict)
        except ZenlyticUnsupportedError as e:
            errors.append(_error_func(f"In view {mf_semantic_model['name']} {str(e)}"))

    for metric in mf_metrics:
        try:
            metric_dict, added_measures = convert_mf_metric_to_zenlytic_measure(
                metric, all_measures, primary_key_mapping
            )
            zenlytic_data["fields"].append(metric_dict)
            zenlytic_data["fields"].extend(added_measures)
        except ZenlyticUnsupportedError as e:
            errors.append(_error_func(f"In view {mf_semantic_model['name']} {str(e)}"))

    # entities to identifiers
    for entity in mf_semantic_model["entities"]:
        if "name" in entity:
            identifier = convert_mf_entity_to_zenlytic_identifier(entity, fields=zenlytic_data["fields"])
            zenlytic_data["identifiers"].append(identifier)

    return zenlytic_data, errors


def convert_mf_dimension_to_zenlytic_dimension(mf_dimension: dict):
    if "expr" in mf_dimension:
        sql = append_table_reference(mf_dimension["expr"])
    else:
        sql = "${TABLE}." + mf_dimension["name"]

    field_dict = {"name": mf_dimension["name"], "sql": sql}

    if mf_dimension["type"].lower() == "time":
        field_dict["field_type"] = "dimension_group"
        field_dict["type"] = "time"
        field_dict["timeframes"] = ["raw", "date", "week", "month", "quarter", "year", "month_of_year"]

    elif mf_dimension["type"].lower() == "categorical":
        field_dict["field_type"] = "dimension"
        field_dict["type"] = "string"
        field_dict["searchable"] = True
    else:
        raise ZenlyticUnsupportedError(
            f"field conversion failed for {mf_dimension['name']}: Dimension type {mf_dimension['type']} not"
            " supported"
        )

    if description := mf_dimension.get("description"):
        field_dict["description"] = description

    if label := mf_dimension.get("label"):
        field_dict["label"] = label

    if mf_dimension.get("meta") and isinstance(mf_dimension["meta"], dict):
        field_dict = {**field_dict, **mf_dimension["meta"].get("zenlytic", {})}

    if mf_dimension.get("config", {}).get("meta") and isinstance(mf_dimension["config"]["meta"], dict):
        field_dict = {**field_dict, **mf_dimension["config"]["meta"].get("zenlytic", {})}

    return field_dict


def convert_mf_measure_to_zenlytic_measure(mf_measure: dict):
    field_dict = {"name": mf_measure["name"], "type": mf_measure["agg"], "field_type": "measure"}
    if "expr" in mf_measure:
        field_dict["sql"] = append_table_reference(str(mf_measure["expr"]))
    else:
        field_dict["sql"] = "${TABLE}." + mf_measure["name"]

    if not mf_measure.get("create_metric", False):
        field_dict["hidden"] = True
        # If we are not creating this metric directly, we need to set an underscore
        # in front of the metric name to stop collisions with metrics that have
        # the exact same name
        field_dict["name"] = "_" + field_dict["name"]

    if field_dict["type"] == "sum_boolean":
        field_dict["type"] = "sum"
        field_dict["sql"] = f"CAST({field_dict['sql']} AS INT)"

    if field_dict["type"] == "percentile":
        field_dict["type"] = "percentile"
        field_dict["percentile"] = int(float(mf_measure["agg_params"]["percentile"]) * 100)
        if mf_measure["agg_params"]["use_discrete_percentile"]:
            raise ZenlyticUnsupportedError(
                f"discrete percentile is not supported for the measure {mf_measure['name']}"
            )

    if canon_date := mf_measure.get("agg_time_dimension"):
        field_dict["canon_date"] = canon_date

    if description := mf_measure.get("description"):
        field_dict["description"] = description

    if label := mf_measure.get("label"):
        field_dict["label"] = label

    if "non_additive_dimension" in mf_measure:
        field_dict["non_additive_dimension"] = {
            **mf_measure["non_additive_dimension"],
            # We need to add the dimension group to the non_additive_dimension reference
            "name": mf_measure["non_additive_dimension"]["name"] + "_raw",
        }

    if mf_measure.get("config", {}).get("meta") and isinstance(mf_measure["config"]["meta"], dict):
        field_dict = {**field_dict, **mf_measure["config"]["meta"].get("zenlytic", {})}

    return field_dict


def convert_mf_entity_to_zenlytic_identifier(mf_entity: dict, fields: list = []):
    # if expr is a simple string, use it as the sql otherwise use it as given as a sql snippet
    entity_dict = {
        "name": mf_entity["name"],
        "type": "primary" if mf_entity["type"] in {"unique", "natural", "primary"} else "foreign",
    }
    if "expr" in mf_entity:
        sql_expr = append_table_reference(str(mf_entity["expr"]))
    else:
        sql_expr = "${TABLE}." + mf_entity["name"]
    entity_dict["sql"] = sql_expr

    for f in fields:
        # If the sql expression equals the field name, use a reference to the field
        if "${TABLE}." + f["name"] == sql_expr:
            entity_dict["sql"] = "${" + f["name"] + "}"
            break

    if mf_entity.get("config", {}).get("meta") and isinstance(mf_entity["config"]["meta"], dict):
        entity_dict = {**entity_dict, **mf_entity["config"]["meta"].get("zenlytic", {})}

    return entity_dict


def convert_mf_metric_to_zenlytic_measure(
    mf_metric: dict, measures: list, primary_key_mapping: dict
) -> tuple:
    """This returns a list because metrics with filters applied can
    result in an additional measure(s) being created
    """
    metric_dict = {
        "name": mf_metric["name"],
        "label": mf_metric.get("label", mf_metric["name"].replace("_", " ").title()),
        "field_type": "measure",
    }

    additional_measures = []
    if mf_metric["type"].lower() == MetricflowMetricTypes.cumulative:
        raise ZenlyticUnsupportedError(
            f"metric conversion failed for {mf_metric['name']}: It is a cumulative metric, which is not"
            " supported."
        )

    elif mf_metric["type"].lower() == MetricflowMetricTypes.simple:
        measure_name = get_name_or_string_literal(mf_metric["type_params"]["measure"])
        associated_measure = _get_measure(measure_name, measures, metric_name=mf_metric["name"])
        metric_dict, _ = apply_filter_to_metric(
            associated_measure, mf_metric, primary_key_mapping, extra_metric_params=metric_dict
        )

    elif mf_metric["type"].lower() == MetricflowMetricTypes.ratio:
        metric_dict["type"] = "ratio"
        numerator = mf_metric["type_params"]["numerator"]
        denominator = mf_metric["type_params"]["denominator"]
        numerator_measure_name = get_name_or_string_literal(numerator)
        denominator_measure_name = get_name_or_string_literal(denominator)

        # If there's a filter, re-write the sql to include the filter
        if isinstance(numerator, dict) and "filter" in numerator:
            associated_numerator = _get_measure(numerator["name"], measures, metric_name=mf_metric["name"])
            numerator_dict, numerator_measures = apply_filter_to_metric(
                associated_numerator,
                numerator,
                primary_key_mapping,
                new_measure_name=mf_metric["name"] + "_numerator",
            )
            numerator_sql = "${" + numerator_dict["name"] + "}"
            additional_measures.extend(numerator_measures)
        else:
            numerator_sql = "${" + numerator_measure_name + "}"

        # If there's a filter, re-write the sql to include the filter
        if isinstance(denominator, dict) and "filter" in denominator:
            associated_denominator = _get_measure(
                denominator["name"], measures, metric_name=mf_metric["name"]
            )
            denominator_dict, denominator_measures = apply_filter_to_metric(
                associated_denominator,
                denominator,
                primary_key_mapping,
                new_measure_name=mf_metric["name"] + "_denominator",
            )
            denominator_sql = "${" + denominator_dict["name"] + "}"
            additional_measures.extend(denominator_measures)
        else:
            denominator_sql = "${" + denominator_measure_name + "}"

        metric_dict["sql"] = numerator_sql + " / " + denominator_sql
        metric_dict["type"] = "number"

    elif mf_metric["type"].lower() == MetricflowMetricTypes.derived:
        metric_dict["type"] = "number"
        expr = mf_metric["type_params"]["expr"]
        referenced_metrics = mf_metric["type_params"]["metrics"]

        # Sort metrics by length in descending order to avoid substring replacement issues
        # This ensures longer names are replaced first (e.g., total_gross_revenue_from_advertising before total_gross_revenue)
        for metric in sorted(referenced_metrics, key=lambda x: len(x.get("name", "")), reverse=True):
            if "alias" in metric and "filter" not in metric:
                # Use word boundaries to ensure we only replace whole words/identifiers
                pattern = r"\b" + re.escape(metric["alias"]) + r"\b"
                expr = re.sub(pattern, "${" + metric["name"] + "}", expr)
            elif "alias" in metric and "filter" in metric:
                associated_measure = _get_measure(metric["name"], measures, metric_name=mf_metric["name"])
                measure_dict, added_measures = apply_filter_to_metric(
                    associated_measure,
                    metric,
                    primary_key_mapping,
                    new_measure_name=mf_metric["name"] + f"_{metric['alias']}",
                )
                additional_measures.extend(added_measures)
                pattern = r"\b" + re.escape(metric["alias"]) + r"\b"
                expr = re.sub(pattern, "${" + measure_dict["name"] + "}", expr)
            else:
                # If there is no alias and no filters we just need to add reference syntax
                pattern = r"\b" + re.escape(metric["name"]) + r"\b"
                expr = re.sub(pattern, "${" + metric["name"] + "}", expr)
        metric_dict["sql"] = expr

    else:
        raise ZenlyticUnsupportedError(f"Metric type {mf_metric['type']} not supported")

    if description := mf_metric.get("description"):
        metric_dict["description"] = description

    if "agg_time_dimension" in mf_metric:
        metric_dict["canon_date"] = mf_metric["agg_time_dimension"]

    if mf_metric.get("config", {}).get("meta") and isinstance(mf_metric["config"]["meta"], dict):
        metric_dict = {**metric_dict, **mf_metric["config"]["meta"].get("zenlytic", {})}

    return metric_dict, additional_measures


def _get_measure(measure_name: str, measures: list, metric_name: str):
    try:
        return next((m for m in measures if m["name"] == measure_name))
    except StopIteration:
        raise ZenlyticUnsupportedError(
            f"could not find associated measure {measure_name} referenced in metric {metric_name}"
        )


def apply_filter_to_metric(
    mf_measure: dict,
    mf_metric: dict,
    primary_key_mapping: dict,
    extra_metric_params: dict = {},
    new_measure_name: str = None,
):
    measure_dict = convert_mf_measure_to_zenlytic_measure(mf_measure)
    hidden = not mf_metric.get("config", {}).get("enabled", True)
    metric_dict = {**measure_dict, **extra_metric_params, "hidden": hidden}

    # If there's a filter, re-write the sql to include the filter
    additional_measures = []
    if "filter" in mf_metric:
        try:
            metric_dict["sql"] = apply_filter_to_sql(
                metric_dict["sql"], mf_metric["filter"], primary_key_mapping
            )
        except ValueError as e:
            raise ZenlyticUnsupportedError(f"metric conversion failed for {mf_metric['name']}: {str(e)}")
        if new_measure_name:
            metric_dict["name"] = new_measure_name
            additional_measures.append(metric_dict)
    return metric_dict, additional_measures


def apply_filter_to_sql(sql, filter, primary_key_mapping: dict):
    filter_sql = _extract_filter_sql(filter, primary_key_mapping)
    return f"case when {filter_sql} then {sql} else null end"


def _extract_filter_sql(filter_string, primary_key_mapping: dict):
    """A filter will look like
    "{{ Dimension('order__is_food_order') }} = True
    We want to turn it into a valid filter statement like ${order.is_food_order} = True
    We do NOT currently support Metric or Entity type filters
    """
    if "Entity(" in filter_string:
        raise ValueError("Entity type filters are not supported")
    if "Metric(" in filter_string:
        raise ValueError("Metric type filters are not supported")
    matches = re.findall(r"{{\s*Dimension\('([^']+)'\)\s*}}", filter_string)
    for match in matches:
        column_name = match.replace("__", ".")

        if "." in column_name:
            primary_key_reference, field_name = column_name.split(".")
            if primary_key_reference in primary_key_mapping:
                column_name = primary_key_mapping[primary_key_reference] + "." + field_name

        replacement = "${" + column_name + "}"
        filter_string = filter_string.replace(f"Dimension('{match}')", replacement)

    # First get the dimension name and grain
    time_dim_matches = re.findall(
        r"""TimeDimension\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)""", filter_string
    )
    for match in time_dim_matches:
        column_name = match[0].replace("__", ".")
        if "." in column_name:
            primary_key_reference, field_name = column_name.split(".")
            if primary_key_reference in primary_key_mapping:
                column_name = primary_key_mapping[primary_key_reference] + "." + field_name

        time_grain = match[1].replace("day", "date")

        replacement = "${" + f"{column_name}_{time_grain}" + "}"
        filter_string = filter_string.replace(f"TimeDimension('{match[0]}', '{match[1]}')", replacement)
    return filter_string.replace("{{", "").replace("}}", "")


def append_table_reference(sql: str):
    try:
        # Parse the SQL to identify column references
        parsed = sqlglot.parse_one(sql.strip())

        # Transform column references to include ${TABLE}. prefix
        def transform_columns(node):
            if isinstance(node, sqlglot.expressions.Column) and not node.table:
                # Only add ${TABLE}. if the column doesn't already have a table reference
                node.set("table", sqlglot.expressions.Identifier(this="${TABLE}"))
            return node

        # Apply the transformation
        transformed = parsed.transform(transform_columns)
        return str(transformed)
    except Exception:
        # Fallback to simple string concatenation if parsing fails
        return sql.strip()


def get_name_or_string_literal(s):
    if isinstance(s, str):
        return s
    elif isinstance(s, dict):
        return s["name"]
    else:
        raise ValueError(f"Invalid type: {type(s)}")


def sql_has_operations(sql: str) -> bool:
    return any(op in sql for op in ["+", "-", "*", "/", "(", ")", " ", ","])


def convert_yml_to_dict(path):
    yaml = ruamel.yaml.YAML(typ="rt")
    yaml.version = (1, 1)
    with open(path, "r") as f:
        yaml_dict = yaml.load(f)
    return yaml_dict


def extract_inner_text(s):
    match = re.search(r"ref\('(.*)'\)", s)
    if match:
        return match.group(1)
    return None


def zenlytic_views_to_yaml(zenlytic_models, zenlytic_views, directory: str = None, write_to_file=True):
    view_directory = os.path.join(directory, "views") if directory else "./views"
    model_directory = os.path.join(directory, "models") if directory else "./models"

    if not os.path.exists(view_directory) and write_to_file:
        os.makedirs(view_directory)

    if not os.path.exists(model_directory) and write_to_file:
        os.makedirs(model_directory)

    zenlytic_yaml = []
    for zenlytic_file in zenlytic_models + zenlytic_views:
        # write the yaml to views/model_name.yml
        if write_to_file:
            if "original_file_path" in zenlytic_file:
                file_path = zenlytic_file["original_file_path"]
            else:
                file_path = f"{zenlytic_file['name']}_{zenlytic_file['type']}.yml"

            if zenlytic_file["type"] == "model":
                write_to_path = os.path.join(model_directory, file_path)
            else:
                write_to_path = os.path.join(view_directory, file_path)
            # write the yaml to views/model_name.yml
            dump_yaml_to_file(zenlytic_file, write_to_path)

        # add the yaml string to views_yaml
        zenlytic_yaml.append(dump_yaml_to_file(zenlytic_file))

    return zenlytic_yaml


def dump_yaml_to_file(data, path: str = None):
    filtered_data = {k: v for k, v in data.items() if not k.startswith("_")}
    if path is None:
        return ruamel.yaml.dump(filtered_data, Dumper=ruamel.yaml.RoundTripDumper)
    else:
        with open(path, "w") as f:
            ruamel.yaml.dump(filtered_data, f, Dumper=ruamel.yaml.RoundTripDumper)


def read_mf_project_files(models_folder: str):
    """Returns a list of all the yml files in the Metricflow project.
    Args:
        models_folder (str): The path to the models folder (usually project_name/models)
    """
    # Make sure models_folder ends with a slash to properly join paths
    if not models_folder.endswith("/"):
        models_folder = models_folder + "/"

    yml_files = glob(f"{models_folder}**/*.yml", recursive=True)
    yaml_files = glob(f"{models_folder}**/*.yaml", recursive=True)
    return yml_files + yaml_files
