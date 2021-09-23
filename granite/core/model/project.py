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
        self._models = models
        self._views = views

    def __repr__(self):
        text = "models" if len(self._models) != 1 else "model"
        return f"<Project {len(self._models)} {text}>"

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
        return [Model(m) for m in self._models]

    def get_model(self, model_name: str) -> Model:
        return next((m for m in self.models() if m.name == model_name), None)

    def explores(self) -> list:
        return [Explore({**e, "model": m}, project=self) for m in self.models() for e in m.explores]

    def get_explore(self, explore_name: str) -> Explore:
        return next((e for e in self.explores() if e.name == explore_name), None)

    def views_with_explore(self, explore_name: str):
        explore = self.get_explore(explore_name)
        view_names = [explore.from_] + [j.name for j in explore.joins()]
        return [v for v in self.views(explore=explore) if v.name in view_names]

    def views(self, explore: Explore = None) -> list:
        return [View({**v, "explore": explore}, project=self) for v in self._views]

    def get_view(self, view_name: str, explore: Explore = None) -> View:
        return next((v for v in self.views(explore=explore) if v.name == view_name), None)

    def fields(self, explore_name: str = None, view_name: str = None) -> list:
        if explore_name is None and view_name is None:
            return self._all_fields()
        elif view_name and explore_name:
            return self._view_fields(view_name, explore=self.get_explore(explore_name))
        elif view_name:
            return self._view_fields(view_name)
        else:
            return self._explore_fields(explore_name)

    def _all_fields(self):
        return [f for v in self.views() for f in v.fields()]

    def _view_fields(self, view_name: str, explore: Explore = None):
        view = self.get_view(view_name, explore=explore)
        return [field for field in view.fields()]

    def _explore_fields(self, explore_name: str):
        valid_views = self.views_with_explore(explore_name)
        return [f for v in valid_views for f in v.fields()]

    def get_field(self, field_name: str, explore_name: str = None, view_name: str = None) -> Field:
        # Handle the case where the explore syntax is passed: explore_name.field_name
        if "." in field_name:
            specified_view_name, field_name = field_name.split(".")
            if view_name and specified_view_name != view_name:
                raise ValueError(
                    f"You specificed two different view names {specified_view_name} and {view_name}"
                )
            view_name = specified_view_name

        field_name = field_name.lower()
        fields = self.fields(explore_name=explore_name, view_name=view_name)
        matching_fields = [f for f in fields if f.equal(field_name)]
        return self._matching_field_handler(matching_fields, field_name, explore_name, view_name)

    def get_explore_from_field(self, field_name: str):
        # If it's specified this is really easy
        if "." in field_name:
            view_name, _ = field_name.split(".")
            return view_name

        # If it's not we have to check all explores to make sure the field isn't ambiguously referenced
        all_fields_with_explore_duplicates = []
        for explore in self.explores():
            for view in self.views_with_explore(explore_name=explore.name):
                all_fields_with_explore_duplicates.extend(view.fields())

        matching_fields = [f for f in all_fields_with_explore_duplicates if f.name == field_name]
        match = self._matching_field_handler(matching_fields, field_name)
        return match.view.explore.name

    def _matching_field_handler(
        self, matching_fields: list, field_name: str, explore_name: str = None, view_name: str = None
    ):
        if len(matching_fields) == 1:
            return matching_fields[0]

        elif len(matching_fields) > 1:
            matching_names = [f"{f.view.name}.{f.name}" for f in matching_fields]
            raise ValueError(
                f"""Multiple fields found for the name {field_name}, {matching_names} please specify a
                view name like this: view_name.field_name"""
            )
        elif field_name == "count" and (explore_name or view_name):
            definition = {"type": "count", "name": "count", "field_type": "measure"}
            if explore_name and not view_name:
                view_name = self.get_explore(explore_name).from_
            return Field(definition, view=self.get_view(view_name))
        else:
            err_msg = f"Field {field_name} not found"
            if explore_name:
                err_msg += f" in explore {explore_name}"
            if view_name:
                err_msg += f" in view {view_name}"
            err_msg += ", please check that this field exists. "
            err_msg += "If this is a dimension group specify the group parameter, if not already specified, "
            err_msg += "for example, with a dimension group named 'order' with timeframes: [raw, date, month]"
            err_msg += " specify 'order_raw' or 'order_date' or 'order_month'"
            raise ValueError(err_msg)
