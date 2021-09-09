import lkml

from .github_repo import GithubRepo


class LookMLProjectReader:
    def __init__(self, repo: GithubRepo):
        self.repo = repo
        self._models = []
        self._views = []

    def load(self) -> None:
        self.repo.fetch()
        self.load_models()
        self.load_views()
        self.repo.delete()

    def load_models(self):
        for fn in self.repo.search(pattern="*.model.*"):
            model_name = self._parse_model_name(fn)
            self._models.append({**self.read_lkml_file(fn), "name": model_name, "type": "model"})

    def load_views(self):
        for fn in self.repo.search(pattern="*.view.*"):
            views = self.read_lkml_file(fn).get("views", [])
            self._views.extend([self.standardize_view(v) for v in views])

    def standardize_view(self, view: dict):
        # Get all fields under the same key "fields"
        measures = self._fields_with_type(view.pop("measures", []), "measure")
        dimensions = self._fields_with_type(view.pop("dimensions", []), "dimension")
        dimension_groups = self._fields_with_type(view.pop("dimension_groups", []), "dimension_group")

        fields = measures + dimensions + dimension_groups
        return {**view, "type": "view", "fields": fields}

    @staticmethod
    def _fields_with_type(fields: list, field_type: str):
        return [{**f, "field_type": field_type} for f in fields]

    @staticmethod
    def read_lkml_file(path: str):
        with open(path, "r") as file:
            lkml_dict = lkml.load(file)
        return lkml_dict

    @staticmethod
    def _parse_model_name(model_path: str) -> str:
        return model_path.split("/")[-1].replace(".model.", "").replace("lkml", "").replace("lookml", "")
