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
        elif repo_type == "metrics_layer":
            models, views = self._load_metrics_layer(repo)
        else:
            raise TypeError(f"Unknown repo type: {repo_type}, valid values are 'metrics_layer', 'lookml'")
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
