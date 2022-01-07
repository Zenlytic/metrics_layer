import json
import os
from copy import deepcopy

import lkml
import yaml

from metrics_layer.core.utils import merge_nested_dict

from .github_repo import BaseRepo


class ProjectReader:
    def __init__(self, repo: BaseRepo, additional_repo: BaseRepo = None):
        self.base_repo = repo
        self.additional_repo = additional_repo
        self.multiple_repos = self.additional_repo is not None
        self.version = 1
        self.unloaded = True
        self._models = []
        self._views = []

    @property
    def models(self):
        if self.unloaded:
            self.load()
        return self._models

    @property
    def views(self):
        if self.unloaded:
            self.load()
        return self._views

    def dump(self, path: str):
        for model in self.models:
            file_name = model["name"] + "_model.yml"
            models_folder = os.path.join(path, "models/")
            if os.path.exists(models_folder):
                file_path = os.path.join(models_folder, file_name)
            else:
                file_path = os.path.join(path, file_name)
            with open(file_path, "w") as f:
                yaml.dump(model, f)

        for view in self.views:
            file_name = view["name"] + "_view.yml"
            views_folder = os.path.join(path, "views/")
            if os.path.exists(views_folder):
                file_path = os.path.join(views_folder, file_name)
            else:
                file_path = os.path.join(path, file_name)
            with open(file_path, "w") as f:
                yaml.dump(view, f)

    def load(self) -> None:
        base_models, base_views = self._load_repo(self.base_repo)
        if self.multiple_repos:
            additional_models, additional_views = self._load_repo(self.additional_repo)
            self._models = self._merge_objects(base_models, additional_models)
            self._views = self._merge_objects(base_views, additional_views)
        else:
            self._models = base_models
            self._views = base_views

        self.unloaded = False

    def _load_repo(self, repo: BaseRepo):
        repo.fetch()
        repo_type = repo.get_repo_type()
        if repo_type == "lookml":
            models, views = self._load_lookml(repo)
        elif repo_type == "dbt":
            models, views = self._load_dbt(repo)
        elif repo_type == "metrics_layer":
            models, views = self._load_metrics_layer(repo)
        else:
            raise TypeError(
                f"Unknown repo type: {repo_type}, valid values are 'metrics_layer', 'lookml', 'dbt'"
            )
        repo.delete()
        return models, views

    def _load_lookml(self, repo: BaseRepo):
        models = []
        for fn in repo.search(pattern="*.model.*"):
            model_name = self._parse_model_name(fn)
            model = self.read_lkml_file(fn)
            models.append(self._standardize_model({**model, "name": model_name, "type": "model"}))

        views = []
        for fn in repo.search(pattern="*.view.*"):
            file_views = self.read_lkml_file(fn).get("views", [])
            views.extend([self._standardize_view(v) for v in file_views])

        return models, views

    def _load_dbt(self, repo: BaseRepo):
        self.project_name = self._get_dbt_project_name(repo.folder)
        self._dump_profiles_file(repo.folder, self.project_name, repo.warehouse_type)
        self._generate_manifest_json(repo.folder)

        manifest_files = repo.search(pattern="manifest.json")
        if len(manifest_files) > 1:
            raise ValueError("found multiple manifest.json files for your dbt project")
        if len(manifest_files) == 0:
            raise ValueError("could not find a manifest.json file for your dbt project")

        with open(manifest_files[0], "r") as f:
            manifest = json.load(f)

        models, views = self._parse_dbt_manifest(manifest)
        return models, views

    def _parse_dbt_manifest(self, manifest: dict):
        views = self._make_dbt_views(manifest)
        models = self._make_dbt_models([v["name"] for v in views])
        return models, views

    def _make_dbt_views(self, manifest: dict):
        metrics = [self._make_dbt_metric(m) for m in manifest["metrics"].values()]
        view_keys = [k for k in manifest["nodes"].keys() if "model." in k]

        views = []
        for view_key in view_keys:
            view_raw = manifest["nodes"][view_key]
            view_metrics = [m for m in metrics if view_raw["name"] == m.get("model")]
            if len(view_metrics) > 0:
                view = self._make_dbt_view(view_raw, view_metrics)
                views.append(view)

        return views

    def _make_dbt_view(self, view: dict, view_metrics: list):
        unique_timestamps = list({m["timestamp"] for m in view_metrics})
        # Pick the longest time grain
        time_grains = list(sorted(view_metrics, key=lambda x: len(x["time_grains"])))[-1]

        if len(unique_timestamps) > 1:
            raise ValueError(
                "cannot handle dbt metrics with different primary timestamps in a view / dbt model"
            )
        dimension_group = self._make_dbt_dimension_group(unique_timestamps[0], time_grains)
        metrics = []
        for m in view_metrics:
            m.pop("model", None)
            m.pop("timestamp", None)
            m.pop("time_grains", None)
            metrics.append(m)

        extra = view["meta"] if view["meta"] != {} else view.get("config", {}).get("meta", {})
        dimensions = [
            self._make_dbt_dimension(d) for d in view.get("columns").values() if d.get("is_dimension")
        ]
        default_date = unique_timestamps[0]
        fields = dimensions + [dimension_group] + metrics
        view_dict = {
            "version": 1,
            "type": "view",
            "name": view["name"],
            "description": view.get("description"),
            "row_label": extra.get("row_label"),
            "sql_table_name": f"{view['schema']}.{view['name']}",
            "default_date": default_date,
            "extra": extra,
            "fields": fields,
        }
        return view_dict

    @staticmethod
    def _make_dbt_dimension_group(name: str, time_grains: list):
        return {
            "field_type": "dimension_group",
            "name": name,
            "type": "time",
            "datatype": "date",
            "timeframes": [t if t != "day" else "date" for t in time_grains],
            "sql": "${TABLE}." + name,
        }

    @staticmethod
    def _make_dbt_dimension(dimension: dict):
        return {
            "field_type": "dimension",
            "name": dimension["name"],
            "type": "string",
            "label": dimension.get("label"),
            "sql": "${TABLE}." + dimension["name"],
            "description": dimension.get("description"),
            "extra": dimension.get("meta", {}),
            "value_format_name": dimension.get("meta", {}).get("value_format_name"),
        }

    @staticmethod
    def _make_dbt_metric(metric: dict):
        if len(metric["sql"].split(" ")) > 1:
            raise ValueError(
                "We do not currently support dbt sql statements that are more than an identifier"
            )
        if any(f["operator"] != "equal_to" for f in metric.get("filters", [])):
            raise ValueError("We do not currently support dbt filter statements that are not equal_to")
        metric_dict = {
            "name": metric["name"],
            "model": metric["model"].replace("ref('", "").replace("')", ""),
            "timestamp": metric["timestamp"],
            "time_grains": metric["time_grains"],
            "field_type": "measure",
            "type": metric["type"],
            "label": metric.get("label"),
            "description": metric.get("description"),
            "sql": "${TABLE}." + metric["sql"],
            "extra": metric.get("meta", {}),
            "filters": [{"field": f["field"], "value": f["value"]} for f in metric.get("filters", [])],
        }
        return metric_dict

    def _make_dbt_models(self, view_names: list):
        model = {"version": 1, "type": "model", "name": self.project_name, "connection": self.project_name}
        model["explores"] = [{"name": view_name} for view_name in view_names]
        return [model]

    def _get_dbt_project_name(self, project_dir: str):
        dbt_project = self.read_yaml_file(os.path.join(project_dir, "dbt_project.yml"))
        return dbt_project["name"]

    @staticmethod
    def _dump_profiles_file(project_dir: str, project_name: str, warehouse_type: str):
        if warehouse_type == "SNOWFLAKE":
            params = {
                "type": "snowflake",
                "account": "fake-url.us-east-1",
                "user": "fake",
                "password": "fake",
                "warehouse": "fake",
                "database": "fake",
                "schema": "fake",
            }
        elif warehouse_type == "BIGQUERY":
            params = {
                "type": "bigquery",
                "method": "service-account",
                "project": "fake",
                "dataset": "fake",
                "keyfile": "fake",
            }
        else:
            raise NotImplementedError()

        profiles = {
            project_name: {"target": "temp", "outputs": {"temp": {**params}}},
            "config": {"send_anonymous_usage_stats": False},
        }
        with open(os.path.join(project_dir, "profiles.yml"), "w") as f:
            yaml.dump(profiles, f)

    @staticmethod
    def _generate_manifest_json(project_dir: str):
        from dbt.main import handle_and_check

        handle_and_check(["ls", "--project-dir", project_dir, "--profiles-dir", project_dir])

    def _load_metrics_layer(self, repo: BaseRepo):
        models, views = [], []
        file_names = repo.search(pattern="*.yml") + repo.search(pattern="*.yaml")
        for fn in file_names:
            yaml_dict = self.read_yaml_file(fn)

            # Handle keyerror
            if "type" not in yaml_dict:
                raise ValueError("All MetricsLayer config files must have a type")

            yaml_type = yaml_dict["type"]

            if yaml_type == "model":
                models.append(yaml_dict)
            elif yaml_type == "view":
                views.append(yaml_dict)
            else:
                raise ValueError(
                    f"Unknown MetricsLayer file type '{yaml_type}' options are 'model' or 'view'"
                )

        return models, views

    def _standardize_view(self, view: dict):
        # Get all fields under the same key "fields"
        measures = self._fields_with_type(view.pop("measures", []), "measure")
        dimensions = self._fields_with_type(view.pop("dimensions", []), "dimension")
        dimension_groups = self._fields_with_type(view.pop("dimension_groups", []), "dimension_group")

        fields = [self._standardize_field(f) for f in measures + dimensions + dimension_groups]
        return {**view, "type": "view", "fields": fields}

    def _standardize_model(self, model: dict):
        model["explores"] = [self._standardize_explore(e) for e in model.get("explores", [])]
        return model

    def _standardize_explore(self, explore: dict):
        if "always_filter" in explore:
            filters = explore["always_filter"].pop("filters__all")
            explore["always_filter"]["filters"] = self._standardize_filters(filters)
        return explore

    def _standardize_field(self, field: dict):
        if "filters__all" in field:
            filters = field.pop("filters__all")
            field["filters"] = self._standardize_filters(filters)
        return field

    def _standardize_filters(self, filters: list):
        clean_filters = []
        for f in filters:
            if isinstance(f, list):
                for nested_filter in f:
                    clean_filters.append(self.__clean_filter(nested_filter))
            else:
                clean_filters.append(self.__clean_filter(f))
        return clean_filters

    def __clean_filter(self, filter_dict: dict):
        # OG looker filter pattern
        if "field" in filter_dict and "value" in filter_dict:
            return filter_dict

        # New Looker filter pattern
        field = list(filter_dict.keys())[0]
        value = list(filter_dict.values())[0]
        return {"field": field, "value": value}

    def _merge_objects(self, base_objects: list, additional_objects: list):
        results = []
        for obj in base_objects:
            additional_obj = self._get_from_list(obj["name"], additional_objects)
            if additional_obj:
                final_obj = merge_nested_dict(base=deepcopy(obj), additional=deepcopy(additional_obj))
            else:
                final_obj = obj
            results.append(final_obj)
        return results

    @staticmethod
    def _get_from_list(name: str, objects: list):
        return next((i for i in objects if i["name"] == name), None)

    @staticmethod
    def read_lkml_file(path: str):
        with open(path, "r") as file:
            lkml_dict = lkml.load(file)
        return lkml_dict

    @staticmethod
    def _fields_with_type(fields: list, field_type: str):
        return [{**f, "field_type": field_type} for f in fields]

    @staticmethod
    def _parse_model_name(model_path: str) -> str:
        return model_path.split("/")[-1].replace(".model.", "").replace("lkml", "").replace("lookml", "")

    @staticmethod
    def read_yaml_file(path: str):
        with open(path, "r") as file:
            yaml_dict = yaml.safe_load(file)
        return yaml_dict
