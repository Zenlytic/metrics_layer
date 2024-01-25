import os

import ruamel.yaml
import yaml


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

    @staticmethod
    def read_yaml_if_exists(file_path: str):
        if os.path.exists(file_path):
            return ProjectReaderBase.read_yaml_file(file_path)
        return None

    @staticmethod
    def read_yaml_file(path: str):
        with open(path, "r") as f:
            yaml_dict = yaml.safe_load(f)
        return yaml_dict

    @staticmethod
    def dump_yaml_file(data: dict, path: str):
        with open(path, "w") as f:
            ruamel.yaml.dump(data, f, Dumper=ruamel.yaml.RoundTripDumper)

    def load(self) -> None:
        raise NotImplementedError()
