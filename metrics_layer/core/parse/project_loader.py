import os

from metrics_layer.core.model.project import Project
from metrics_layer.core.parse.connections import (
    ConnectionType,
    BaseConnection,
    BigQueryConnection,
    RedshiftConnection,
    PostgresConnection,
    SnowflakeConnection,
)

from .github_repo import GithubRepo, LocalRepo
from .manifest import Manifest
from .project_reader_base import ProjectReaderBase
from .project_reader_dbt import dbtProjectReader
from .project_reader_metrics_layer import MetricsLayerProjectReader


class ConfigError(Exception):
    pass


class ProjectLoader:
    def __init__(self, location: str, branch: str = "master", connections: list = [], **kwargs):
        self.kwargs = kwargs
        self.repo = self._get_repo(location, branch, kwargs)
        self._raw_connections = connections
        self._project = None
        self._user = None

    def load(self):
        self._connections = self.load_connections(self._raw_connections)
        self._project = self._load_project()
        return self._project

    @property
    def zenlytic_project(self):
        reader = ProjectReaderBase(repo=self.repo)
        return reader.zenlytic_project if reader.zenlytic_project else {}

    @staticmethod
    def profiles_path():
        home_dir = os.path.expanduser("~")
        profiles_path = os.path.join(home_dir, ".dbt", "profiles.yml")
        return profiles_path

    def get_branch_options(self):
        return self.repo.branch_options

    def _load_project(self):
        self.repo.fetch()
        repo_type = self.repo.get_repo_type()
        if repo_type == "dbt":
            reader = dbtProjectReader(repo=self.repo)
        elif repo_type == "metrics_layer":
            reader = MetricsLayerProjectReader(self.repo)
        else:
            raise TypeError(f"Unknown repo type: {repo_type}, valid values are 'metrics_layer', 'dbt'")

        models, views, dashboards = reader.load()

        self.repo.delete()

        project = Project(
            models=models,
            views=views,
            dashboards=dashboards,
            connection_lookup={c.name: c.type for c in self._connections},
            manifest=Manifest(reader.manifest),
        )
        return project

    def _get_repo(self, location: str, branch: str, kwargs: dict):
        # Config is passed explicitly: this gets first priority
        if location is not None:
            return self._get_repo_from_location(location, branch, kwargs)

        # Next look for environment variables
        repo = self._get_repo_from_environment(kwargs)
        if repo:
            return repo

        raise ConfigError(
            """ We could not find a valid configuration in the environment. Try following the
            documentation (https://docs.zenlytic.com/docs/development_environment/development_environment)
            to properly set your environment variables, or pass the configuration explicitly
        """
        )

    @staticmethod
    def _get_repo_from_location(location: str, branch: str, kwargs: dict):
        if ProjectLoader._is_local(location):
            return LocalRepo(repo_path=location, **kwargs)
        return GithubRepo(repo_url=location, branch=branch, **kwargs)

    @staticmethod
    def _get_repo_from_environment(kwargs: dict):
        prefix = "METRICS_LAYER"
        location = os.getenv(f"{prefix}_LOCATION")
        branch = os.getenv(f"{prefix}_BRANCH", "master")
        repo_type = os.getenv(f"{prefix}_REPO_TYPE")
        if location is None:
            return None

        if ProjectLoader._is_local(location):
            return LocalRepo(repo_path=location, repo_type=repo_type, **kwargs)
        return GithubRepo(repo_url=location, branch=branch, repo_type=repo_type, **kwargs)

    @staticmethod
    def _is_local(location: str):
        return "https://" not in location

    @staticmethod
    def load_connections(connections: list):
        class_lookup = {
            ConnectionType.snowflake: SnowflakeConnection,
            ConnectionType.bigquery: BigQueryConnection,
            ConnectionType.redshift: RedshiftConnection,
            ConnectionType.postgres: PostgresConnection,
        }
        results = []
        for connection in connections:
            if isinstance(connection, BaseConnection):
                connection_class = connection
            else:
                connection_type = connection["type"].upper()
                connection_class = class_lookup[connection_type](**connection)
            results.append(connection_class)
        return results

    def get_connections_from_profile(profile_name: str, target: str = None):
        profile_path = ProjectLoader.profiles_path()
        profiles_directory = os.path.dirname(profile_path)
        profiles_dict = dbtProjectReader.read_yaml_if_exists(profile_path)
        if profiles_dict is None:
            raise ConfigError(f"Could not find dbt profiles.yml at {profile_path}")

        profile = profiles_dict.get(profile_name)

        if profile is None:
            raise ConfigError(f"Could not find profile {profile_name} in profiles.yml at {profile_path}")

        if target is None:
            target = profile["target"]

        target_dict = profile["outputs"].get(target)

        if target_dict is None:
            raise ConfigError(
                f"Could not find target {target} in profile {profile_name} in profiles.yml at {profile_path}"
            )

        return [{**target_dict, "directory": profiles_directory, "name": profile_name}]
