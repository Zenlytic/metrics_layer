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
        return self.read_yaml_if_exists(os.path.join(self.repo.folder, "zenlytic_project.yml"))

    @property
    def dbt_project(self):
        dbt_project_fn = self.search_dbt_project("dbt_project.yml")
        if len(dbt_project_fn) > 1:
            raise QueryError(f"found multiple dbt_project.yml files for your dbt project: {dbt_project_fn}")
        elif len(dbt_project_fn) == 0 and self.repo.get_repo_type() == "dbt":
            raise QueryError("no dbt_project.yml file found in your dbt project")
        elif len(dbt_project_fn) == 1:
            return self.read_yaml_if_exists(dbt_project_fn[0])
        return None

    def search_dbt_project(self, pattern: str):
        folder = self.repo.dbt_path if self.repo.dbt_path else self.repo.folder
        return BaseRepo.glob_search(folder, pattern)

    def generate_manifest_json(self, project_dir: str, profiles_dir: str):
        if profiles_dir is None:
            profiles_dir = project_dir
            if not os.path.exists(os.path.join(profiles_dir, "profiles.yml")):
                self._dump_profiles_file(profiles_dir, self.dbt_project["profile"])

        self._run_dbt("ls", project_dir=project_dir, profiles_dir=profiles_dir)

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

    @staticmethod
    def _run_dbt(cmd: str, project_dir: str, profiles_dir: str):
        os.system(f"dbt {cmd} --project-dir {project_dir} --profiles-dir {profiles_dir}")

    def read_yaml_if_exists(self, file_path: str):
        if os.path.exists(file_path):
            return self.read_yaml_file(file_path)
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
