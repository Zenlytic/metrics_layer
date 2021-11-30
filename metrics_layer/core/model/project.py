from .explore import Explore
from .field import Field
from .model import Model
from .view import View


class Project:
    """
    Higher level abstraction for the whole project
    """

    def __init__(self, models: list, views: list, looker_env: str = None, connection_lookup: dict = {}):
        self._models = models
        self._views = views
        self.looker_env = looker_env
        self.connection_lookup = connection_lookup

    def __repr__(self):
        text = "models" if len(self._models) != 1 else "model"
        return f"<Project {len(self._models)} {text}>"

    def validate(self):
        all_errors = []
        for explore in self.explores():
            errors = explore.validate_fields()
            all_errors.extend(errors)
        return all_errors

    def models(self) -> list:
        return [Model(m) for m in self._models]

    def get_model(self, model_name: str) -> Model:
        return next((m for m in self.models() if m.name == model_name), None)

    def explores(self, show_hidden: bool = True) -> list:
        explores = [Explore({**e, "model": m}, project=self) for m in self.models() for e in m.explores]
        if show_hidden:
            return explores
        return [e for e in explores if e.hidden == "no" or not e.hidden]

    def get_explore(self, explore_name: str) -> Explore:
        try:
            return next((e for e in self.explores() if e.name == explore_name))
        except StopIteration:
            pass
        raise ValueError(f"Could not find explore {explore_name} in config")

    def views_with_explore(self, explore_name: str):
        explore = self.get_explore(explore_name)
        view_names = [explore.from_] + [j.name for j in explore.joins()]
        return [v for v in self.views(explore=explore) if v.name in view_names]

    def views(self, explore_name: str = None, explore: Explore = None) -> list:
        if explore_name:
            return self.views_with_explore(explore_name=explore_name)
        return [View({**v, "explore": explore}, project=self) for v in self._views]

    def get_view(self, view_name: str, explore: Explore = None) -> View:
        return next((v for v in self.views(explore=explore) if v.name == view_name), None)

    def fields(self, explore_name: str = None, view_name: str = None, show_hidden: bool = True) -> list:
        if explore_name is None and view_name is None:
            return self._all_fields(show_hidden)
        elif view_name and explore_name:
            return self._view_fields(
                view_name, explore=self.get_explore(explore_name), show_hidden=show_hidden
            )
        elif view_name:
            return self._view_fields(view_name, show_hidden=show_hidden)
        else:
            return self._explore_fields(explore_name, show_hidden=show_hidden)

    def _all_fields(self, show_hidden: bool):
        return [f for v in self.views() for f in v.fields(show_hidden=show_hidden)]

    def _view_fields(self, view_name: str, explore: Explore = None, show_hidden: bool = True):
        view = self.get_view(view_name, explore=explore)
        if not view:
            plus_explore = f" in explore {explore.name}" if explore else ""
            raise ValueError(f"Could not find a view matching the name {view_name}{plus_explore}")
        return [field for field in view.fields(show_hidden=show_hidden)]

    def _explore_fields(self, explore_name: str, show_hidden: bool):
        valid_views = self.views_with_explore(explore_name)
        return [f for v in valid_views for f in v.fields(show_hidden=show_hidden)]

    def get_field(self, field_name: str, explore_name: str = None, view_name: str = None) -> Field:
        # Handle the case where the explore syntax is passed: explore_name.field_name
        if "." in field_name:
            specified_explore_name, specified_view_name, field_name = Field.field_name_parts(field_name)
            if view_name and specified_view_name != view_name:
                raise ValueError(
                    f"You specificed two different view names {specified_view_name} and {view_name}"
                )
            if specified_explore_name and explore_name and specified_explore_name != explore_name:
                raise ValueError(
                    f"You specificed two different explore names {specified_explore_name} and {explore_name}"
                )
            view_name = specified_view_name
            if specified_explore_name:
                explore_name = specified_explore_name

        field_name = field_name.lower()
        fields = self.fields(explore_name=explore_name, view_name=view_name)
        matching_fields = [f for f in fields if f.equal(field_name)]
        return self._matching_field_handler(matching_fields, field_name, explore_name, view_name)

    def get_explore_from_field(self, field_name: str):
        # If it's specified this is really easy
        if "." in field_name:
            explore_name, _, _ = Field.field_name_parts(field_name)
            if explore_name is not None:
                return explore_name

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
            matching_names = [self._fully_qualified_name(f) for f in matching_fields]
            explore_text = f", in explore {explore_name}" if explore_name else ""
            view_text = f", in view {view_name}" if view_name else ""
            err_msg = (
                f"Multiple fields found for the name {field_name}{explore_text}{view_text}"
                f" - those fields were {matching_names} \n\nPlease specify a "
                "view name like this: 'view_name.field_name' or "
                "an explore and view like this 'explore_name.view_name.field_name'"
                "\n\nor pass the argument 'explore_name' to the function, to set the explore"
            )
            raise ValueError(err_msg)
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
            err_msg += ", please check that this field exists. \n\n"
            err_msg += "If this is a dimension group specify the group parameter, if not already specified, "
            err_msg += "for example, with a dimension group named 'order' with timeframes: [raw, date, month]"
            err_msg += " specify 'order_raw' or 'order_date' or 'order_month'"
            raise ValueError(err_msg)

    @staticmethod
    def _fully_qualified_name(field: Field):
        name = f"{field.view.name}.{field.name}"
        if field.view.explore:
            name = f"{field.view.explore.name}.{name}"
        return name
