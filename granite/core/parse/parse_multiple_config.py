from copy import deepcopy

from .github_repo import BaseRepo
from .parse_granite_config import GraniteProjectReader
from .parse_lookml import LookMLProjectReader


class GraniteMultipleProjectReader:
    def __init__(
        self, base_repo: BaseRepo, base_repo_type: str, additional_repo: BaseRepo, additional_repo_type: str
    ):
        self.unloaded = True
        self.base_repo = base_repo
        self.base_repo_type = base_repo_type
        self.additional_repo = additional_repo
        self.additional_repo_type = additional_repo_type

    @property
    def models(self):
        if self.unloaded:
            self.load()

        models = []
        for model in self._base_models:
            additional_model = self._get_additional_model(model["name"])
            if additional_model:
                final_model = self._merge_nested(base=deepcopy(model), additional=deepcopy(additional_model))
            else:
                final_model = model
            models.append(final_model)

        return models

    @property
    def views(self):
        if self.unloaded:
            self.load()

        views = []
        for view in self._base_views:
            additional_view = self._get_additional_view(view["name"])
            if additional_view:
                final_view = self._merge_nested(base=deepcopy(view), additional=deepcopy(additional_view))
            else:
                final_view = view
            views.append(final_view)
        return views

    def load(self) -> None:
        base_reader = self._get_parser(self.base_repo, self.base_repo_type)
        base_reader.load()
        self._base_models = base_reader.models
        self._base_views = base_reader.views

        additional_reader = self._get_parser(self.additional_repo, self.additional_repo_type)
        additional_reader.load()
        self._additional_models = additional_reader.models
        self._additional_views = additional_reader.views

        self.unloaded = False

    def _merge_nested(self, base, additional):
        for key, val in additional.items():
            if isinstance(val, dict):
                if key in base and isinstance(base[key], dict):
                    self._merge_nested(base[key], additional[key])

            elif isinstance(val, list):
                if key in base and isinstance(base[key], list):
                    additional[key] = self._merge_list(base[key], additional[key])

            else:
                if key in base:
                    additional[key] = base[key]

        for key, val in base.items():
            if key not in additional:
                additional[key] = val

        return additional

    @staticmethod
    def _merge_list(base_list, additional_list):
        final_list = []
        added = []
        for item in base_list:
            if not isinstance(item, dict):
                final_list.append(item)
                continue

            additional_item = next((i for i in additional_list if i["name"] == item["name"]), None)
            if additional_item:
                added.append(item["name"])
                final_list.append({**additional_item, **item})
            else:
                final_list.append(item)
        for item in additional_list:
            if isinstance(item, dict) and item["name"] not in added:
                final_list.append(item)

        return final_list

    def _get_additional_model(self, model_name: str):
        return next((m for m in self._additional_models if m["name"] == model_name), None)

    def _get_additional_view(self, view_name: str):
        return next((v for v in self._additional_views if v["name"] == view_name), None)

    def _get_parser(self, repo: BaseRepo, repo_type: str):
        if repo_type == "lookml":
            return LookMLProjectReader(repo=repo)
        elif repo_type == "granite":
            return GraniteProjectReader(repo=repo)
        else:
            raise ValueError(f"Invalid repo type: {repo_type} - valid types include 'lookml', 'granite'")
