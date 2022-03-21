import os
from copy import deepcopy

from metrics_layer.core.model.project import Project
from metrics_layer.core.parse.connections import (
    BigQueryConnection,
    ConnectionType,
    RedshiftConnection,
    SnowflakeConnection,
)

from .github_repo import GithubRepo, LocalRepo, LookerGithubRepo
from .manifest import Manifest
from .project_reader import ProjectReader


class ConfigError(Exception):
    pass


class MetricsLayerConfiguration:
    def __init__(
        self,
        repo_config: dict = None,
        additional_repo_config: dict = None,
        connections: list = [],
        target: str = None,
    ):
        self.profiles_path = None
        self.env_name = target
        self.looker_env = self.env_name
        self.repo, conns = self._resolve_config(repo_config, prefix="METRICS_LAYER", raise_exception=True)
        self.additional_repo, addtl_conns = self._resolve_config(
            additional_repo_config, prefix="METRICS_LAYER_ADDITIONAL"
        )
        self._connections = self._parse_connections(connections) + conns + addtl_conns
        self._project = None
        self._user = None

    def set_user(self, user: dict):
        self._user = user
        if self._project is not None:
            self._project.set_user(self._user)

    @staticmethod
    def get_metrics_layer_configuration(config=None, target: str = None):
        if config:
            return config
        return MetricsLayerConfiguration(target=target)

    @property
    def project(self):
        if self._project is None:
            self.load()
        return self._project

    def connections(self):
        return self._connections

    def get_connection(self, connection_name: str):
        try:
            return next((c for c in self.connections() if c.name == connection_name))
        except StopIteration:
            raise ConfigError(f"Could not find connection named {connection_name} in {self.connections()}")

    def add_connection(self, connection_dict: dict):
        parsed_connections = self._parse_connections([connection_dict])
        self._connections.extend(parsed_connections)

    def load(self):
        self._project = self._get_project()
        self._project.set_user(self._user)

    def dump(self, path: str = "./"):
        reader = ProjectReader(repo=self.repo, additional_repo=self.additional_repo)
        reader.load()
        reader.dump(path)

    def _get_project(self):
        if self.profiles_path:
            profiles_dir = os.path.dirname(self.profiles_path)
        else:
            profiles_dir = None
        reader = ProjectReader(
            repo=self.repo, additional_repo=self.additional_repo, profiles_dir=profiles_dir
        )
        reader.load()
        connection_lookup = {c.name: c.type for c in self.connections()}
        project = Project(
            models=reader.models,
            views=reader.views,
            dashboards=reader.dashboards,
            looker_env=self.looker_env,
            connection_lookup=connection_lookup,
            manifest=Manifest(reader.manifest),
        )
        return project

    def _resolve_config(self, config: dict, prefix: str, raise_exception: bool = False):
        # Config is passed explicitly: this gets first priority
        if config is not None and isinstance(config, dict):
            return self._get_config_repo(config), []

        # Next look for environment variables
        repo = self._get_repo_from_environment(prefix)
        if repo:
            return repo, []

        # Finally look for config file
        repo, connections = self._get_repo_from_config_file(prefix, config, target_name=self.env_name)
        if repo:
            return repo, connections

        if raise_exception:
            raise ConfigError(
                """ We could not find a valid configuration in the environment. Try following the
                documentation (TODO) to properly set your environment variables, or pass the
                configuration explicitly as a dictionary
            """
            )
        return None, []

    @staticmethod
    def _get_config_repo(config: dict):
        if all(a in config for a in ["repo_path"]):
            return LocalRepo(**config)

        if all(a in config for a in ["repo_url", "branch"]):
            return GithubRepo(**config)

        if all(a in config for a in ["looker_url", "client_id", "client_secret", "project_name"]):
            return LookerGithubRepo(**config)

        raise ConfigError(
            f"""Could not initialize any known config with the arguments passed:
            {list(config.keys())} We either initialize a local repo with the following arguments:
            'repo_path': 'path/to/my/repo'
            or a direct Github connection with following arguments:
            'repo_url': https://username:personal_access_token@github.com/yourorg/yourrepo.git, 'branch': dev
            or through the Looker API with the following arguments:
            'looker_url': https://company.cloud.looker.com, 'client_id': aefafasdasd,
            'client_secret': adfawdada, 'project_name': yourcompany

            Note: dbt repos require an additional 'warehouse_type' argument
            """
        )

    @staticmethod
    def _get_repo_from_environment(prefix: str):
        warehouse_type = os.getenv(f"{prefix}_WAREHOUSE_TYPE")
        local_repo_param = os.getenv(f"{prefix}_REPO_PATH")
        if local_repo_param:
            repo_type = os.getenv(f"{prefix}_REPO_TYPE")
            return LocalRepo(repo_path=local_repo_param, repo_type=repo_type, warehouse_type=warehouse_type)

        repo_params = [os.getenv(f"{prefix}_{a}") for a in ["REPO_URL", "BRANCH"]]
        if all(repo_params):
            repo_type = os.getenv(f"{prefix}_REPO_TYPE")
            return GithubRepo(
                repo_url=repo_params[0],
                branch=repo_params[1],
                repo_type=repo_type,
                warehouse_type=warehouse_type,
            )

        looker_repo_params = [
            os.getenv(f"{prefix}_{a}") for a in ["LOOKER_URL", "CLIENT_ID", "CLIENT_SECRET", "PROJECT_NAME"]
        ]
        if all(looker_repo_params):
            config = {
                "looker_url": looker_repo_params[0],
                "client_id": looker_repo_params[1],
                "client_secret": looker_repo_params[2],
                "project_name": looker_repo_params[3],
                "repo_type": "lookml",
            }
            return LookerGithubRepo(**config)

    def _get_repo_from_config_file(self, prefix: str, config_profile_name: str, target_name: str = None):
        if config_profile_name is None:
            return None, []

        clean_prefix = "additional_" if "additional" in prefix.lower() else ""

        metrics_layer_profiles_directory = self.get_metrics_layer_profiles_directory()
        self.profiles_path = os.path.join(metrics_layer_profiles_directory, "profiles.yml")

        if not os.path.exists(self.profiles_path):
            raise ConfigError(
                f"""MetricsLayer could not find the profiles.yml file in the directory
             {self.profiles_path}, please check that path to ensure the file exists.
             If this does not look like the path you specified, make sure you haven't set
             the environment variable METRICS_LAYER_PROFILES_DIR to another value
             which may be overriding the default (~, your home directory)"""
            )

        all_profiles = ProjectReader.read_yaml_file(self.profiles_path)

        try:
            profile = all_profiles[config_profile_name]
        except KeyError:
            raise ConfigError(
                f"MetricsLayer could not find the profile "
                f"{config_profile_name} in options {list(all_profiles.keys())} "
                f"located in the profiles.yml file at {self.profiles_path}"
            )

        if "target" not in profile:
            raise ConfigError(
                "You need to specify a default target in the "
                f"file {self.profiles_path} for profile {config_profile_name}"
            )

        if "outputs" not in profile:
            raise ConfigError(
                "You need to specify outputs in your profile so MetricsLayer can "
                f"connect to your database for profile {config_profile_name}"
            )

        try:
            target_name = target_name if target_name else profile["target"]
            target = profile["outputs"][target_name]
        except KeyError:
            raise ConfigError(
                f"MetricsLayer could not find the target "
                f"{target_name} in outputs {list(profile['outputs'].keys())} "
                f"located in the profiles.yml file at {self.profiles_path}"
            )

        if "looker_env" in target:
            self.looker_env = target["looker_env"]

        repo_type = target.get(f"{clean_prefix}repo_type")
        raw_connection = deepcopy(target)
        warehouse_type = raw_connection.get("type")

        # Github repo
        if all(k in target for k in [f"{clean_prefix}repo_url", f"{clean_prefix}branch"]):
            repo = GithubRepo(
                repo_url=target[f"{clean_prefix}repo_url"],
                branch=target[f"{clean_prefix}branch"],
                repo_type=repo_type,
                warehouse_type=warehouse_type,
            )

        # Looker API
        looker_keys = [
            f"{clean_prefix}looker_url",
            f"{clean_prefix}client_id",
            f"{clean_prefix}client_secret",
            f"{clean_prefix}project_name",
        ]
        if all(k in target for k in looker_keys):
            looker_args = {k: target[k] for k in looker_keys}
            repo = LookerGithubRepo(**looker_args, repo_type="lookml")

        # Local repo
        path_arg = f"{clean_prefix}repo_path"
        if path_arg in target:
            if os.path.isabs(target[path_arg]):
                path = target[path_arg]
            else:
                path = os.path.abspath(
                    os.path.join(metrics_layer_profiles_directory, os.path.expanduser(target[path_arg]))
                )
        else:
            config_path = os.path.join(os.getcwd(), "zenlytic_project.yml")
            if os.path.exists(config_path):
                config = ProjectReader.read_yaml_file(config_path)
                relative_path = config.get("folder", "./")
                path = os.path.abspath(relative_path)
            else:
                path = os.getcwd()
        repo = LocalRepo(repo_path=path, repo_type=repo_type, warehouse_type=warehouse_type)

        connection = {
            **raw_connection,
            "name": config_profile_name,
            "directory": metrics_layer_profiles_directory,
        }
        return repo, MetricsLayerConfiguration._parse_connections([connection])

    @staticmethod
    def get_all_profiles(directory, names_only: bool = False):
        all_profiles = ProjectReader.read_yaml_file(directory)
        if names_only:
            return list(all_profiles.keys())
        return all_profiles

    @staticmethod
    def get_metrics_layer_profiles_directory():
        env_specified_location = os.getenv(f"METRICS_LAYER_PROFILES_DIR")
        if env_specified_location:
            if os.path.isabs(env_specified_location):
                return env_specified_location
            else:
                return os.path.join(os.getcwd(), os.path.abspath(env_specified_location))

        # System default home directory to dbt profiles dir
        home = os.path.expanduser("~")
        location = os.path.join(home, ".dbt/")
        return location

    @staticmethod
    def _parse_connections(connections: list):
        class_lookup = {
            ConnectionType.snowflake: SnowflakeConnection,
            ConnectionType.bigquery: BigQueryConnection,
            ConnectionType.redshift: RedshiftConnection,
        }
        results = []
        for connection in connections:
            connection_type = connection["type"].upper()
            connection_class = class_lookup[connection_type](**connection)
            results.append(connection_class)
        return results
