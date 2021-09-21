import os
import shutil
from glob import glob

import git
import requests

from granite.core import utils

BASE_PATH = os.path.dirname(__file__)


class BaseRepo:
    def delete(self):
        raise NotImplementedError()

    def search(self):
        raise NotImplementedError()

    def fetch(self):
        raise NotImplementedError()


class GithubRepo(BaseRepo):
    def __init__(self, repo_url: str, branch: str) -> None:
        self.repo_name = utils.generate_uuid()
        self.repo_url = repo_url
        self.repo_destination = os.path.join(BASE_PATH, self.repo_name)
        self.folder = f"{self.repo_destination}/"
        self.branch = branch

    def search(self, pattern: str):
        """Example arg: pattern='*.model.*'"""
        return glob(f"{self.folder}**/{pattern}", recursive=True)

    def fetch(self):
        self.fetch_github_repo(self.repo_url, self.repo_destination, self.branch)

    def delete(self, folder: str = None):
        if folder is None:
            folder = self.folder

        if os.path.exists(folder) and os.path.isdir(folder):
            shutil.rmtree(folder)

    @staticmethod
    def fetch_github_repo(repo_url: str, repo_destination: str, branch: str):
        if os.path.exists(repo_destination) and os.path.isdir(repo_destination):
            shutil.rmtree(repo_destination)
        git.Repo.clone_from(repo_url, to_path=repo_destination, branch=branch, depth=1)


class LookerGithubRepo(BaseRepo):
    def __init__(self, looker_url: str, client_id: str, client_secret: str, project_name: str):
        self.looker_url = looker_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.project_name = project_name
        self.repo_url, self.branch = self.get_looker_github_info()
        self.repo = GithubRepo(self.repo_url, self.branch)

    def search(self, pattern: str):
        """Example arg: pattern='*.model.*'"""
        return self.repo.search(pattern=pattern)

    def fetch(self) -> None:
        self.repo.fetch()

    def delete(self, folder: str = None) -> None:
        self.repo.delete(folder=folder)

    def get_looker_github_info(self):
        projects = self.get_looker_projects()
        project = next((p for p in projects if p["name"] == self.project_name))
        return project["git_remote_url"], project["git_production_branch_name"]

    def get_looker_projects(self, base_url: str, client_id: str, client_secret: str):
        token = self.get_looker_oauth_token(self.looker_url, self.client_id, self.client_secret)
        headers = {"Authorization": f"token {token}"}
        response = requests.get(f"{self.looker_url}/api/3.1/projects", headers=headers)
        return response.json()

    @staticmethod
    def get_looker_oauth_token(looker_url, client_id, client_secret):
        data = {"client_id": client_id, "client_secret": client_secret}
        response = requests.post(f"{looker_url}/api/3.1/login", data=data)
        if response.status_code == 403:
            raise ValueError("Looker credentials not valid, please check your credentials")
        return response.json()["access_token"]
