import os
import shutil
from glob import glob

import git
import requests

from metrics_layer.core import utils
from metrics_layer.core.exceptions import QueryError

BASE_PATH = os.path.dirname(__file__)


class BaseRepo:
    def get_repo_type(self):
        if self.repo_type:
            return self.repo_type

        looker_files = list(self.search(pattern="*.model.*"))
        looker_files += list(self.search(pattern="*.view.*"))
        n_looker_files = len(looker_files)

        yaml_files = list(self.search(pattern="*.yml"))
        yaml_files += list(self.search(pattern="*.yaml"))
        n_yaml_files = len(yaml_files)

        # TODO Need to decide if we will support this
        # dbt_files = list(self.search(pattern="dbt_project.yml"))
        # dbt_files += list(self.search(pattern="dbt_project.yml"))
        # n_dbt_files = len(dbt_files)

        # if n_dbt_files > 0:
        #     return "dbt"
        if n_looker_files > n_yaml_files:
            return "lookml"
        return "metrics_layer"

    def delete(self):
        raise NotImplementedError()

    def search(self, pattern: str):
        """Example arg: pattern='*.model.*'"""
        return self.glob_search(self.folder, pattern)

    def fetch(self):
        raise NotImplementedError()

    @staticmethod
    def glob_search(folder: str, pattern: str):
        return [f for f in glob(f"{folder}**/{pattern}", recursive=True) if "venv" not in f]


class LocalRepo(BaseRepo):
    def __init__(
        self, repo_path: str, repo_type: str = None, dbt_path: str = None, warehouse_type: str = None
    ) -> None:
        self.repo_path = repo_path
        self.dbt_path = dbt_path
        if isinstance(dbt_path, str) and dbt_path[-1] != "/":
            self.dbt_path += "/"
        self.repo_type = repo_type
        self.warehouse_type = warehouse_type
        self.folder = f"{os.path.join(os.getcwd(), self.repo_path)}/"
        self.branch_options = []

    def fetch(self):
        pass

    def delete(self):
        pass


class GithubRepo(BaseRepo):
    def __init__(self, repo_url: str, branch: str, repo_type: str = None, warehouse_type: str = None) -> None:
        self.repo_url = repo_url
        self.repo_type = repo_type
        self.warehouse_type = warehouse_type
        self.repo_name = utils.generate_uuid()
        self.repo_destination = os.path.join(BASE_PATH, self.repo_name)
        self.folder = f"{self.repo_destination}/"
        self.dbt_path = self.folder
        self.branch = branch
        self.branch_options = []

    def fetch(self):
        self.fetch_github_repo(self.repo_url, self.repo_destination, self.branch)

        try:
            dynamic_branch_options = []
            g = git.cmd.Git()
            raw = g.ls_remote(self.repo_url)
            for raw_branch_ref in raw.split("\n"):
                if "/heads/" in raw_branch_ref:
                    clean_branch_ref = raw_branch_ref.split("/heads/")[-1]
                    dynamic_branch_options.append(clean_branch_ref)
            self.branch_options = dynamic_branch_options
        except Exception as e:
            print(f"Exception getting branch options: {e}")
            self.branch_options = [self.branch]

    def delete(self, folder: str = None):
        if folder is None:
            folder = self.folder

        if os.path.exists(folder) and os.path.isdir(folder):
            shutil.rmtree(folder)

    @staticmethod
    def fetch_github_repo(repo_url: str, repo_destination: str, branch: str):
        if os.path.exists(repo_destination) and os.path.isdir(repo_destination):
            shutil.rmtree(repo_destination)
        repo = git.Repo.clone_from(repo_url, to_path=repo_destination, branch=branch, depth=1)
        return repo


class LookerGithubRepo(BaseRepo):
    def __init__(
        self, looker_url: str, client_id: str, client_secret: str, project_name: str, repo_type: str = None
    ):
        self.looker_url = looker_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.project_name = project_name
        self.repo_type = repo_type
        self.repo_url, self.branch = self.get_looker_github_info()
        self.repo = GithubRepo(self.repo_url, self.branch)
        self.dbt_path = self.repo.folder
        self.branch_options = []

    def search(self, pattern: str):
        """Example arg: pattern='*.model.*'"""
        return self.repo.search(pattern=pattern)

    def fetch(self) -> None:
        self.repo.fetch()
        self.branch_options = self.repo.branch_options

    def delete(self, folder: str = None) -> None:
        self.repo.delete(folder=folder)

    def get_looker_github_info(self):
        projects = self.get_looker_projects()
        project = next((p for p in projects if p["name"] == self.project_name))
        return project["git_remote_url"], project["git_production_branch_name"]

    def get_looker_projects(self):
        token = self.get_looker_oauth_token(self.looker_url, self.client_id, self.client_secret)
        headers = {"Authorization": f"token {token}"}
        response = requests.get(f"{self.looker_url}/api/3.1/projects", headers=headers)
        return response.json()

    @staticmethod
    def get_looker_oauth_token(looker_url, client_id, client_secret):
        data = {"client_id": client_id, "client_secret": client_secret}
        response = requests.post(f"{looker_url}/api/3.1/login", data=data)
        if response.status_code == 403:
            raise QueryError("Looker credentials not valid, please check your credentials")
        return response.json()["access_token"]
