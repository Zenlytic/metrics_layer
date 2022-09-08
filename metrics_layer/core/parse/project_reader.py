import json
import os
from collections import OrderedDict, defaultdict, Counter
from copy import deepcopy

import lkml
import ruamel.yaml
import yaml

from metrics_layer.core.utils import merge_nested_dict
from metrics_layer.core.exceptions import QueryError

from .github_repo import BaseRepo

# from ruamel import yaml


class ProjectReader:
    def __init__(self, repo: BaseRepo, additional_repo: BaseRepo = None, profiles_dir: str = None):
        self.base_repo = repo
        self.additional_repo = additional_repo
        self.multiple_repos = self.additional_repo is not None
        self.profiles_dir = profiles_dir
        self.version = 1
        self.unloaded = True
        self.has_dbt_project = False
        self.manifest = {}
        self._models = []
        self._views = []
        self._dashboards = []

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

    @property
    def dashboards(self):
        if self.unloaded:
            self.load()
        return self._dashboards

    def dump(self, path: str, views_only: bool = False):
        for model in self.models:
            if views_only:
                break
            file_name = model["name"] + "_model.yml"
            models_folder = os.path.join(path, "models/")
            if os.path.exists(models_folder):
                file_path = os.path.join(models_folder, file_name)
            else:
                file_path = os.path.join(path, file_name)
            self._dump_yaml_file(model, file_path)

        for view in self.views:
            file_name = view["name"] + "_view.yml"
            views_folder = os.path.join(path, "views/")
            if os.path.exists(views_folder):
                file_path = os.path.join(views_folder, file_name)
            else:
                file_path = os.path.join(path, file_name)
            self._dump_yaml_file(view, file_path)

    def _sort_view(self, view: dict):
        view_key_order = [
            "version",
            "type",
            "name",
            "sql_table_name",
            "default_date",
            "row_label",
            "extends",
            "extension",
            "required_access_grants",
            "sets",
            "fields",
        ]
        extra_keys = [k for k in view.keys() if k not in view_key_order]
        new_view = OrderedDict()
        for k in view_key_order + extra_keys:
            if k in view:
                if k == "fields":
                    new_view[k] = self._sort_fields(view[k])
                else:
                    new_view[k] = view[k]
        return new_view

    def _sort_fields(self, fields: list):
        sort_key = ["dimension", "dimension_group", "measure"]
        sorted_fields = sorted(fields, key=lambda x: sort_key.index(x["field_type"]))
        return [self._sort_field(f) for f in sorted_fields]

    def _sort_field(self, field: dict):
        field_key_order = [
            "name",
            "field_type",
            "type",
            "datatype",
            "hidden",
            "primary_key",
            "label",
            "view_label",
            "description",
            "required_access_grants",
            "value_format_name",
            "drill_fields",
            "sql_distinct_key",
            "tiers",
            "timeframes",
            "intervals",
            "sql_start",
            "sql_end",
            "sql",
            "filters",
            "extra",
        ]
        extra_keys = [k for k in field.keys() if k not in field_key_order]
        new_field = OrderedDict()
        for k in field_key_order + extra_keys:
            if k in field:
                new_field[k] = field[k]
        return new_field

    def _sort_model(self, model: dict):
        model_key_order = [
            "version",
            "type",
            "name",
            "label",
            "connection",
            "fiscal_month_offset",
            "week_start_day",
            "access_grants",
        ]
        extra_keys = [k for k in model.keys() if k not in model_key_order]
        new_model = OrderedDict()
        for k in model_key_order + extra_keys:
            if k in model:
                new_model[k] = model[k]
        return new_model

    @staticmethod
    def _dump_yaml_file(data: dict, path: str):
        with open(path, "w") as f:
            ruamel.yaml.dump(data, f, Dumper=ruamel.yaml.RoundTripDumper)
            # yaml.dump(data, f)

    def load(self) -> None:
        base_models, base_views, base_dashboards = self._load_repo(self.base_repo)
        if self.multiple_repos:
            additional_models, additional_views, additional_dashboards = self._load_repo(self.additional_repo)
            self._models = self._merge_objects(base_models, additional_models)
            self._views = self._merge_objects(base_views, additional_views)
            self._dashboards = base_dashboards + additional_dashboards
        else:
            self._models = base_models
            self._views = base_views
            self._dashboards = base_dashboards

        self.unloaded = False

    def _load_repo(self, repo: BaseRepo):
        repo.fetch()
        repo_type = repo.get_repo_type()
        if repo_type == "lookml":
            models, views, dashboards = self._load_lookml(repo)
        elif repo_type == "dbt":
            models, views, dashboards = self._load_dbt(repo)
        elif repo_type == "metrics_layer":
            models, views, dashboards = self._load_metrics_layer(repo)
        else:
            raise TypeError(
                f"Unknown repo type: {repo_type}, valid values are 'metrics_layer', 'lookml', 'dbt'"
            )
        repo.delete()
        return models, views, dashboards

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

        # Empty list is for currently unsupported dashboards when using the lookml mode
        return models, views, []

    def _load_dbt(self, repo: BaseRepo):
        dbt_proj = self._get_dbt_project_file(repo.folder)
        self.project_name = dbt_proj["name"]

        self._dump_profiles_file(repo.folder, self.project_name)
        self._generate_manifest_json(repo.folder, self.profiles_dir)

        self.manifest = self._load_manifest_json(repo)
        proj = self._read_yaml_if_exists(os.path.join(repo.folder, "zenlytic_project.yml"))
        if proj:
            self.profile_name = proj.get("profile", dbt_proj.get("profile", self.project_name))
        else:
            self.profile_name = dbt_proj.get("profile", self.project_name)

        models, views = self._parse_dbt_manifest(self.manifest)
        dashboard_paths = (
            [os.path.join(repo.folder, dp) for dp in proj.get("dashboard-paths", [])] if proj else []
        )
        return models, views, self._load_dbt_dashboards(dashboard_paths)

    def _load_dbt_dashboards(self, dashboard_folders: list):
        if not dashboard_folders:
            return []

        dashboards = []
        for folder in dashboard_folders:
            dashboards.extend(self._gather_dashboards(folder))

        return dashboards

    def _gather_dashboards(self, dashboard_folder: str):
        file_names = BaseRepo.glob_search(dashboard_folder, "*.yml")
        file_names += BaseRepo.glob_search(dashboard_folder, "*.yaml")

        dashboards = []
        for fn in file_names:
            yaml_dict = self.read_yaml_file(fn)

            # Handle keyerror
            if "type" not in yaml_dict:
                print(f"WARNING: file {fn} is missing a type")

            if yaml_dict.get("type") == "dashboard":
                dashboards.append(yaml_dict)
        return dashboards

    def _load_manifest_json(self, repo):
        manifest_files = self.search_dbt_project(repo, pattern="manifest.json")
        if len(manifest_files) > 1:
            raise QueryError(f"found multiple manifest.json files for your dbt project: {manifest_files}")
        if len(manifest_files) == 0:
            raise QueryError("could not find a manifest.json file for your dbt project")

        with open(manifest_files[0], "r") as f:
            manifest = json.load(f)
        return manifest

    @staticmethod
    def search_dbt_project(repo, pattern: str):
        folder = repo.dbt_path if repo.dbt_path else repo.folder
        return BaseRepo.glob_search(folder, pattern)

    def _parse_dbt_manifest(self, manifest: dict):
        views = self._make_dbt_views(manifest)
        models = self._make_dbt_models()
        return models, views

    def _make_dbt_models(self):
        model = {"version": 1, "type": "model", "name": self.project_name, "connection": self.profile_name}
        return [model]

    def _make_dbt_views(self, manifest: dict):
        metrics = [self._make_dbt_metric(m) for m in manifest["metrics"].values()]
        view_keys = [k for k in manifest["nodes"].keys() if "model." in k]

        views = []
        for view_key in view_keys:
            view_raw = manifest["nodes"][view_key]
            view_metrics = [m for m in metrics if view_raw["name"] == m.get("model")]
            view = self._make_dbt_view(view_raw, view_metrics)
            views.append(view)

        return views

    def _make_dbt_view(self, view: dict, view_metrics: list):
        dimension_group_dict, metric_timestamps = defaultdict(set), []
        for m in view_metrics:
            dimension_group_dict[m["timestamp"]] |= set(m["time_grains"])
            metric_timestamps.append(m["timestamp"])

        dimension_groups = []
        for timestamp, time_grains in dimension_group_dict.items():
            matching_column = view.get("columns", {}).get(timestamp, {})
            dimension_groups.append(self._make_dbt_dimension_group(timestamp, time_grains, matching_column))

        metrics = []
        for m in view_metrics:
            m.pop("model", None)
            m.pop("timestamp", None)
            m.pop("time_grains", None)
            metrics.append(m)

        meta = view["meta"] if view["meta"] != {} else view.get("config", {}).get("meta", {})
        dimension_group_names = [d["name"] for d in dimension_groups]
        dimensions = [
            self._make_dbt_dimension(d)
            for d in view.get("columns", {}).values()
            if d["name"] not in dimension_group_names
        ]
        default_date = self._dbt_default_date(metric_timestamps)
        view_dict = {
            "version": 1,
            "type": "view",
            "name": view["name"],
            "model_name": self.profile_name,
            "description": view.get("description"),
            "row_label": meta.get("row_label"),
            "sql_table_name": f"ref('{view['name']}')",
            "default_date": default_date,
            "fields": dimensions + dimension_groups + metrics,
            **meta,
        }
        return view_dict

    @staticmethod
    def _dbt_default_date(metric_timestamps: list):
        if len(metric_timestamps) > 0:
            return Counter(metric_timestamps).most_common()[0][0]
        return

    @staticmethod
    def _make_dbt_dimension_group(name: str, time_grains: list, matching_column: dict):
        return {
            "field_type": "dimension_group",
            "name": name,
            "type": "time",
            "timeframes": [ProjectReader._convert_in_lookup(t, {"day": "date"}) for t in time_grains],
            "sql": "${TABLE}." + name,
            **matching_column.get("meta", {}),
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
            "hidden": not dimension.get("is_dimension"),
            **dimension.get("meta", {}),
        }

    @staticmethod
    def _make_dbt_metric(metric: dict):
        metric_dict = {
            "name": metric["name"],
            "model": ProjectReader._clean_model(metric.get("model")),
            "timestamp": metric["timestamp"],
            "time_grains": metric["time_grains"],
            "field_type": "measure",
            "type": ProjectReader._convert_in_lookup(metric["type"], {"expression": "number"}),
            "label": metric.get("label"),
            "description": metric.get("description"),
            "sql": ProjectReader._convert_dbt_sql(metric),
            **metric.get("meta", {}),
        }
        return metric_dict

    @staticmethod
    def _clean_model(dbt_model_name: str):
        if dbt_model_name:
            return dbt_model_name.replace("ref('", "").replace("')", "")
        return None

    @staticmethod
    def _convert_in_lookup(value: str, lookup: dict):
        if value in lookup:
            return lookup[value]
        return value

    def _convert_dbt_sql(metric: dict):
        if metric["type"] == "expression":
            base_metric_sql = metric["sql"]
            for metric_group in metric["metrics"]:
                for metric in metric_group:
                    base_metric_sql = base_metric_sql.replace(metric, "${" + metric + "}")
            return base_metric_sql
        else:
            metric_sql = "${TABLE}." + metric["sql"]
            return ProjectReader._apply_dbt_filters(metric_sql, metric.get("filters", []))

    def _apply_dbt_filters(metric_sql: str, filters: list):
        if len(filters) == 0:
            return metric_sql
        core_filter = " and ".join([ProjectReader._dbt_filter_to_sql(f) for f in filters])
        return f"case when {core_filter} then {metric_sql} else null end"

    @staticmethod
    def _dbt_filter_to_sql(dbt_filter: dict):
        sql = " ".join(["${" + dbt_filter["field"] + "}", dbt_filter["operator"], dbt_filter["value"]])
        return sql

    def _get_dbt_project_file(self, project_dir: str):
        dbt_project = self.read_yaml_file(os.path.join(project_dir, "dbt_project.yml"))
        return dbt_project

    def _read_yaml_if_exists(self, file_path: str):
        if os.path.exists(file_path):
            return self.read_yaml_file(file_path)
        return None

    def _dump_profiles_file(self, project_dir: str, project_name: str):
        # It doesn't matter the warehouse type here because we're just compiling the models
        params = {
            "type": "snowflake",
            "account": "fake-url.us-east-1",
            "user": "fake",
            "password": "fake",
            "warehouse": "fake",
            "database": "fake",
            "schema": "fake",
        }

        profiles = {
            project_name: {"target": "temp", "outputs": {"temp": {**params}}},
            "config": {"send_anonymous_usage_stats": False},
        }
        self._dump_yaml_file(profiles, os.path.join(project_dir, "profiles.yml"))

    def _generate_manifest_json(self, project_dir: str, profiles_dir: str):

        if profiles_dir is None:
            profiles_dir = project_dir
            if not os.path.exists(os.path.join(profiles_dir, "profiles.yml")):
                project = self._get_dbt_project_file(project_dir)
                self._dump_profiles_file(profiles_dir, project["profile"])

        self._run_dbt("ls", project_dir=project_dir, profiles_dir=profiles_dir)

    @staticmethod
    def _run_dbt(cmd: str, project_dir: str, profiles_dir: str):
        os.system(f"dbt {cmd} --project-dir {project_dir} --profiles-dir {profiles_dir}")

    def _load_metrics_layer(self, repo: BaseRepo):
        models, views, dashboards = [], [], []
        self.has_dbt_project = len(list(self.search_dbt_project(repo, pattern="dbt_project.yml"))) > 0
        if self.has_dbt_project:
            self._generate_manifest_json(repo.dbt_path, self.profiles_dir)
            self.manifest = self._load_manifest_json(repo)

        file_names = repo.search(pattern="*.yml") + repo.search(pattern="*.yaml")
        for fn in file_names:
            yaml_dict = self.read_yaml_file(fn)

            # Handle keyerror
            if "type" not in yaml_dict:
                print(f"WARNING: file {fn} is missing a type")

            yaml_type = yaml_dict.get("type")

            if yaml_type == "model":
                models.append(yaml_dict)
            elif yaml_type == "view":
                views.append(yaml_dict)
            elif yaml_type == "dashboard":
                dashboards.append(yaml_dict)
            elif yaml_type:
                print(f"WARNING: Unknown file type '{yaml_type}' options are 'model', 'view', or 'dashboard'")

        return models, views, dashboards

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
        with open(path, "r") as f:
            lkml_dict = lkml.load(f)
        return lkml_dict

    @staticmethod
    def _fields_with_type(fields: list, field_type: str):
        return [{**f, "field_type": field_type} for f in fields]

    @staticmethod
    def _parse_model_name(model_path: str) -> str:
        return model_path.split("/")[-1].replace(".model.", "").replace("lkml", "").replace("lookml", "")

    @staticmethod
    def read_yaml_file(path: str):
        with open(path, "r") as f:
            # yaml_dict = ruamel.yaml.load(f, Loader=ruamel.yaml.RoundTripLoader)
            yaml_dict = yaml.safe_load(f)
        return yaml_dict
