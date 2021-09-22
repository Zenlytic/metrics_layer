import os

from granite.core.model.project import Project

from .github_repo import GithubRepo, LookerGithubRepo
from .project_reader import ProjectReader


class ConfigError(Exception):
    pass


class GraniteConfiguration:
    def __init__(self, repo_config: dict = None, additional_repo_config: dict = None, connections: list = []):
        self.repo = self._resolve_config(repo_config, prefix="GRANITE", raise_exception=True)
        self.additional_repo = self._resolve_config(additional_repo_config, prefix="GRANITE_ADDITIONAL")
        self._connections = connections
        self._project = None

    @property
    def project(self):
        if self._project is None:
            self._project = self._get_project()
        return self._project

    def connections(self):
        return self._connections

    def get_connection(self, connection_name: str):
        try:
            return next((c for c in self.connections() if c["name"] == connection_name))
        except StopIteration:
            raise ConfigError(f"Could not find connection named {connection_name} in {self.connections}")

    def _get_project(self):
        reader = ProjectReader(repo=self.repo, additional_repo=self.additional_repo)
        reader.load()
        project = Project(models=reader.models, views=reader.views)
        return project

    def _resolve_config(self, config: dict, prefix: str, raise_exception: bool = False):
        # Config is passed explicitly: this gets first priority
        if config is not None:
            return self._get_config_repo(config)

        # Next look for environment variables
        repo = self._get_repo_from_environment(prefix)
        if repo:
            return repo

        if raise_exception:
            raise ConfigError(
                """ We could not find a valid configuration in the environment. Try following the
                documentation (TODO) to properly set your environment variables, or pass the
                configuration explicitly as a dictionary
            """
            )

    @staticmethod
    def _get_config_repo(config: dict):
        if all(a in config for a in ["repo_url", "branch"]):
            return GithubRepo(**config)

        if all(a in config for a in ["looker_url", "client_id", "client_secret", "project_name"]):
            return LookerGithubRepo(**config)

        raise ConfigError(
            f"""Could not initialize any known config with the arguments passed:
            {list(config.keys())} We either initialize a direct Github connection with following arguments:
            'repo_url': https://username:personal_access_token@github.com/yourorg/yourrepo.git, 'branch': dev
            or through the Looker API with the following arguments:
            'looker_url': https://cloud.company.looker.com, 'client_id': aefafasdasd,
            'client_secret': adfawdada, 'project_name': yourcompany
            """
        )

    @staticmethod
    def _get_repo_from_environment(prefix: str):
        repo_params = [os.getenv(f"{prefix}_{a}") for a in ["REPO_URL", "BRANCH"]]
        if all(repo_params):
            repo_type = os.getenv(f"{prefix}_REPO_TYPE")
            return GithubRepo(repo_url=repo_params[0], branch=repo_params[1], repo_type=repo_type)

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
