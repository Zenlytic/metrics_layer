from .base import GraniteBase
from .explore import Explore
from .field import Field
from .model import Model
from .view import View


class Project(GraniteBase):
    """
    Higher level abstraction for the whole project
    """

    def __init__(self, models: list, views: list):
        self._models = [Model(m) for m in models]
        self._views = [View(v) for v in views]

    def get_design(self, explore_name: str):
        design = {}
        design["explore"] = self.get_explore(explore_name).to_dict()
        views_to_add = [design["explore"]["from"]] + [j["name"] for j in design["explore"]["joins"]]
        design["views"] = []
        for view_name in views_to_add:
            view = self.get_view(view_name).to_dict(explore_to_exclude_by=explore_name)
            design["views"].append(view)
        return design

    def models(self) -> list:
        return self._models

    def get_model(self, model_name: str) -> Model:
        return next((m for m in self.models() if m.name == model_name), None)

    def explores(self) -> list:
        return [Explore({**e, "model": m}) for m in self.models() for e in m.explores]

    def get_explore(self, explore_name: str) -> Explore:
        return next((e for e in self.explores() if e.name == explore_name), None)

    def views(self) -> list:
        return self._views

    def get_view(self, view_name: str) -> View:
        return next((v for v in self.views() if v.name == view_name), None)

    def fields(self, view_name=None) -> list:
        view = self.get_view(view_name)
        return view.fields()

    def get_field(self, field_name: str, view_name=None) -> Field:
        fields = self.fields(view_name=view_name)
        return next((f for f in fields if f.equal(field_name)), None)
