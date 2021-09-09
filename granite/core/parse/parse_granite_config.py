import yaml

from .github_repo import GithubRepo


class GraniteProjectReader:
    def __init__(self, repo: GithubRepo):
        self.repo = repo
        self._models = []
        self._views = []

    def load(self) -> None:
        self.repo.fetch()
        self.load_files()
        self.repo.delete()

    def load_files(self):
        file_names = self.repo.search(pattern="*.yml") + self.repo.search(pattern="*.yaml")
        for fn in file_names:
            yaml_dict = self.read_yaml_file(fn)

            # Handle keyerror
            if "type" not in yaml_dict:
                raise ValueError("All Granite config files must have a type")

            yaml_type = yaml_dict["type"]

            if yaml_type == "model":
                self._models.append(yaml_dict)
            elif yaml_type == "view":
                self._views.append(yaml_dict)
            else:
                raise ValueError(f"Unknown Granite file type '{yaml_type}' options are 'model' or 'view'")

    @staticmethod
    def read_yaml_file(path: str):
        with open(path, "r") as file:
            yaml_dict = yaml.safe_load(file)
        return yaml_dict
