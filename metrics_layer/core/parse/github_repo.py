import os
import shutil
from glob import glob
import yaml

import git

from metrics_layer.core import utils

BASE_PATH = os.path.dirname(__file__)


class BaseRepo:
    def get_repo_type(self):
        if self.repo_type:
            return self.repo_type

        project_files = list(self.search(pattern="zenlytic_project.yaml", folders=[self.folder]))
        project_files += list(self.search(pattern="zenlytic_project.yml", folders=[self.folder]))
        if len(project_files) == 1:
            return self.read_yaml_file(project_files[0]).get("mode", "metrics_layer")
        return "metrics_layer"

    def delete(self):
        raise NotImplementedError()

    def search(self, pattern: str, folders: list = []):
        """Example arg: pattern='*.yml'"""
        return [fn for f in folders for fn in self.glob_search(f, pattern) if "venv" not in fn]

    def fetch(self):
        raise NotImplementedError()

    def get_dbt_path(self):
        pattern = "dbt_project.yml"
        in_root = list(glob(f"{self.folder}/{pattern}"))
        if len(in_root) == 1:
            return self.folder
        in_one_folder_deep = list(glob(f"{self.folder}/*/{pattern}"))
        if len(in_one_folder_deep) == 1:
            return os.path.dirname(in_one_folder_deep[0]) + "/"
        return self.folder

    @staticmethod
    def read_yaml_file(path: str):
        with open(path, "r") as f:
            yaml_dict = yaml.safe_load(f)
        return yaml_dict

    @staticmethod
    def glob_search(folder: str, pattern: str):
        return glob(f"{folder}**/{pattern}", recursive=True)


class LocalRepo(BaseRepo):
    def __init__(self, repo_path: str, repo_type: str = None) -> None:
        self.repo_path = repo_path
        self.repo_type = repo_type
        self.folder = f"{os.path.join(os.getcwd(), self.repo_path)}/"
        self.dbt_path = self.get_dbt_path()
        self.branch_options = []

    def fetch(self):
        pass

    def delete(self):
        pass


class GithubRepo(BaseRepo):
    def __init__(self, repo_url: str, branch: str, repo_type: str = None) -> None:
        self.repo_url = repo_url
        self.repo_type = repo_type
        self.repo_name = utils.generate_uuid()
        self.repo_destination = os.path.join(BASE_PATH, self.repo_name)
        self.folder = f"{self.repo_destination}/"
        self.dbt_path = None
        self.branch = branch
        self.branch_options = []

    def fetch(self):
        self.fetch_github_repo(self.repo_url, self.repo_destination, self.branch)

        self.dbt_path = self.get_dbt_path()
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
