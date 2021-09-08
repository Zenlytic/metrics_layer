import lkml


class LookMLProjectReader:
    def __init__(self, repo: str):
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
            self._models.append({**self.read_lkml_file(fn), "name": model_name})

    def load_views(self):
        for fn in self.repo.search(pattern="*.view.*"):
            views = self.read_lkml_file(fn).get("views", [])
            self._views.extend(views)

    @staticmethod
    def read_lkml_file(path: str):
        with open(path, "r") as file:
            lkml_dict = lkml.load(file)
        return lkml_dict

    @staticmethod
    def _parse_model_name(model_path: str) -> str:
        return model_path.split("/")[-1].replace(".model.", "").replace("lkml", "").replace("lookml", "")
