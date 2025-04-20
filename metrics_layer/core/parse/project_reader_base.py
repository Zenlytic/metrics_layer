import os

import ruamel.yaml

from .github_repo import BaseRepo


class ProjectReaderBase:
    def __init__(self, repo: BaseRepo, profiles_dir: str = None):
        self.repo = repo
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

    @property
    def zenlytic_project(self):
        return self.read_yaml_if_exists(self.zenlytic_project_path)

    @property
    def zenlytic_project_path(self):
        zenlytic_project = self.read_yaml_if_exists(os.path.join(self.repo.folder, "zenlytic_project.yml"))
        if zenlytic_project:
            return os.path.join(self.repo.folder, "zenlytic_project.yml")
        return os.path.join(self.dbt_folder, "zenlytic_project.yml")

    @property
    def dbt_project(self):
        return self.read_yaml_if_exists(os.path.join(self.dbt_folder, "dbt_project.yml"))

    @property
    def dbt_folder(self):
        return self.repo.dbt_path if self.repo.dbt_path else self.repo.folder

    def get_folders(self, key: str, default: str = None, raise_errors: bool = True):
        if not self.zenlytic_project:
            return []

        if key in self.zenlytic_project:
            return [self._abs_path(p) for p in self.zenlytic_project[key]]
        elif raise_errors:
            raise KeyError(
                f"Missing required key '{key}' in zenlytic_project.yml \n"
                "Learn more about setting these keys here: https://docs.zenlytic.com"
            )
        return []

    def _abs_path(self, path: str):
        if not os.path.isabs(path):
            path = os.path.join(self.repo.folder, path)
        return path

    def search_for_yaml_files(self, folders: list):
        file_names = self.repo.search("*.yml", folders) + self.repo.search("*.yaml", folders)
        return list(set(file_names))

    @staticmethod
    def read_yaml_if_exists(file_path: str):
        if os.path.exists(file_path):
            return ProjectReaderBase.read_yaml_file(file_path)
        return None

    @staticmethod
    def read_yaml_file(path: str):
        yaml = ruamel.yaml.YAML(typ="rt")
        yaml.version = (1, 1)
        with open(path, "r") as f:
            yaml_dict = yaml.load(f)
        return yaml_dict

    @staticmethod
    def repr_str(representer, data):
        return representer.represent_str(str(data))

    @staticmethod
    def dump_yaml_file(data: dict, path: str):
        yaml = ruamel.yaml.YAML(typ="rt")
        filtered_data = {k: v for k, v in data.items() if not k.startswith("_")}
        with open(path, "w") as f:
            yaml.dump(filtered_data, f)

    def load(self) -> None:
        raise NotImplementedError()
