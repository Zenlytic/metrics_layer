import json
import os

import ruamel.yaml
import yaml

from metrics_layer.core.exceptions import QueryError

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

    def search_dbt_project(self, pattern: str):
        return BaseRepo.glob_search(self.dbt_folder, pattern)

    def generate_manifest_json(self, project_dir: str, profiles_dir: str):
        dumped_profiles_file = False
        if profiles_dir is None:
            profiles_dir = project_dir
            if not os.path.exists(os.path.join(profiles_dir, "profiles.yml")):
                self._dump_profiles_file(profiles_dir, self.dbt_project["profile"])
                dumped_profiles_file = True

        self._run_dbt("deps", project_dir=project_dir, profiles_dir=profiles_dir)
        self._run_dbt("ls", project_dir=project_dir, profiles_dir=profiles_dir)

        if dumped_profiles_file:
            self._clean_up_profiles_file(profiles_dir)

    def load_manifest_json(self):
        manifest_files = self.search_dbt_project("manifest.json")
        if len(manifest_files) > 1:
            raise QueryError(f"found multiple manifest.json files for your dbt project: {manifest_files}")
        if len(manifest_files) == 0:
            raise QueryError("could not find a manifest.json file for your dbt project")

        with open(manifest_files[0], "r") as f:
            manifest = json.load(f)
        return manifest

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
        self.dump_yaml_file(profiles, os.path.join(project_dir, "profiles.yml"))

    def _clean_up_profiles_file(self, project_dir: str):
        profiles_path = os.path.join(project_dir, "profiles.yml")
        if os.path.exists(profiles_path):
            os.remove(profiles_path)

    @staticmethod
    def _run_dbt(cmd: str, project_dir: str, profiles_dir: str):
        os.system(f"dbt {cmd} --project-dir {project_dir} --profiles-dir {profiles_dir}")

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
