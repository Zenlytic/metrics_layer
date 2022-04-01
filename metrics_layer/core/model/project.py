import functools
import hashlib
import json

from .base import AccessDeniedOrDoesNotExistException
from .dashboard import Dashboard
from .explore import Explore
from .field import Field
from .model import AccessGrant, Model
from .view import View


class Project:
    """
    Higher level abstraction for the whole project
    """

    def __init__(
        self,
        models: list,
        views: list,
        dashboards: list = [],
        looker_env: str = None,
        connection_lookup: dict = {},
        manifest=None,
    ):
        self._models = models
        self._views = views
        self._dashboards = dashboards
        self.looker_env = looker_env
        self.connection_lookup = connection_lookup
        self.manifest = manifest
        self.manifest_exists = manifest and manifest.exists()
        self._user = None

    def __repr__(self):
        text = "models" if len(self._models) != 1 else "model"
        return f"<Project {len(self._models)} {text} user={self._user}>"

    def __hash__(self):
        model_str = json.dumps(self._models, sort_keys=True)
        view_str = json.dumps(self._views, sort_keys=True)
        dash_str = json.dumps(self._dashboards, sort_keys=True)
        conn_str = json.dumps(self.connection_lookup, sort_keys=True)
        user_str = "" if not self._user else json.dumps(self._user, sort_keys=True)
        string_to_hash = model_str + view_str + dash_str + conn_str + user_str + str(self.looker_env)
        result = hashlib.md5(string_to_hash.encode("utf-8"))
        return int(result.hexdigest(), base=16)

    def set_user(self, user: dict):
        self._user = user

    def validate(self):
        all_errors = []
        for model in self.models():
            all_errors.extend(model.collect_errors())

        for explore in self.explores():
            errors = explore.validate_fields()
            all_errors.extend(errors)

        for dashboard in self.dashboards():
            errors = dashboard.collect_errors()
            all_errors.extend(errors)

        return list(sorted(set(all_errors), key=lambda x: all_errors.index(x)))

    def _all_dashboards(self):
        dashboards = []
        for d in self._dashboards:
            dashboard = Dashboard(d, project=self)
            user_allowed = self.can_access_dashboard(dashboard)
            if user_allowed:
                dashboards.append(dashboard)
        return dashboards

    def dashboards(self) -> list:
        return self._all_dashboards()

    def get_dashboard(self, dashboard_name: str) -> Model:
        try:
            return next((d for d in self.dashboards() if d.name == dashboard_name))
        except StopIteration:
            raise AccessDeniedOrDoesNotExistException(
                f"Could not find or you do not have access to dashboard {dashboard_name}",
                object_name=dashboard_name,
                object_type="dashboard",
            )

    def models(self) -> list:
        return [Model(m) for m in self._models]

    def get_model(self, model_name: str) -> Model:
        return next((m for m in self.models() if m.name == model_name), None)

    def access_grants(self):
        return [AccessGrant(g) for m in self.models() for g in m.access_grants]

    def get_access_grant(self, grant_name: str):
        return next((ag for ag in self.access_grants() if ag.name == grant_name), None)

    def can_access_dashboard(self, dashboard: Dashboard):
        return self._can_access_object(dashboard)

    def can_access_explore(self, explore: Explore):
        return self._can_access_object(explore)

    def can_access_join(self, join, explore: Explore):
        can_access_explore = self.can_access_explore(explore)
        return self._can_access_object(join) and can_access_explore

    def can_access_view(self, view: View):
        return self._can_access_object(view)

    def can_access_field(self, field):
        can_access_view = self.can_access_view(field.view)
        return self._can_access_object(field) and can_access_view

    def can_access_merged_field(self, field, explore: Explore):
        can_access_explore = self.can_access_explore(explore)
        return self._can_access_object(field) and can_access_explore

    def _can_access_object(self, obj):
        if self._user is not None:
            if obj.required_access_grants:
                decisions = []
                for grant_name in obj.required_access_grants:
                    grant = self.get_access_grant(grant_name)
                    user_attribute_value = self._user.get(grant.user_attribute)

                    if user_attribute_value is None:
                        decision = True
                    else:
                        decision = user_attribute_value in grant.allowed_values
                    decisions.append(decision)

                # We use all here because the condition between access conditions is AND
                return all(decisions)
        return True

    def _all_explores(self):
        explores = []
        for m in self.models():
            for e in m.explores:
                explore = Explore({**e, "model": m}, project=self)
                user_allowed = self.can_access_explore(explore)
                if user_allowed:
                    explores.append(explore)
        return explores

    def explores(self, show_hidden: bool = True) -> list:
        explores = self._all_explores()
        if show_hidden:
            return explores
        return [e for e in explores if e.hidden == "no" or not e.hidden]

    def get_explore(self, explore_name: str) -> Explore:
        try:
            return next((e for e in self.explores() if e.name == explore_name))
        except StopIteration:
            raise AccessDeniedOrDoesNotExistException(
                f"Could not find or you do not have access to explore {explore_name}",
                object_name=explore_name,
                object_type="explore",
            )

    def _all_views(self, explore):
        views = []
        for v in self._views:
            view = View({**v, "explore": explore}, project=self)
            if self.can_access_view(view):
                views.append(view)
        return views

    def views(self, explore_name: str = None, explore: Explore = None) -> list:
        if explore_name:
            return self.views_with_explore(explore_name=explore_name)
        return self._all_views(explore)

    def views_with_explore(self, explore_name: str = None, explore: Explore = None):
        if explore_name and explore is None:
            explore = self.get_explore(explore_name)
        view_names = [explore.from_] + [j.from_ for j in explore.joins()]
        return [v for v in self.views(explore=explore) if v.name in view_names]

    def get_view(self, view_name: str, explore: Explore = None) -> View:
        try:
            return next((v for v in self.views(explore=explore) if v.name == view_name))
        except StopIteration:
            raise AccessDeniedOrDoesNotExistException(
                f"Could not find or you do not have access to view {view_name}",
                object_name=view_name,
                object_type="view",
            )

    def sets(self, view_name: str = None, explore_name: str = None):
        if explore_name:
            views = self.views(explore_name=explore_name)
        elif view_name:
            try:
                views = [self.get_view(view_name)]
            except AccessDeniedOrDoesNotExistException:
                views = []
        else:
            views = self.views()

        all_sets = []
        for view in views:
            all_sets.extend(view.list_sets())
        return all_sets

    def get_set(self, set_name: str, view_name: str = None):
        if view_name:
            sets = self.sets(view_name=view_name)
        else:
            sets = self.sets()
        return next((s for s in sets if s.name == set_name), None)

    @functools.lru_cache(maxsize=None)
    def fields(
        self,
        explore_name: str = None,
        view_name: str = None,
        show_hidden: bool = True,
        expand_dimension_groups: bool = False,
        show_excluded: bool = False,
    ) -> list:
        if explore_name is None and view_name is None:
            return self._all_fields(show_hidden, expand_dimension_groups)
        elif view_name and explore_name:
            fields = self._explore_fields(explore_name, show_hidden, expand_dimension_groups, show_excluded)
            return [f for f in fields if f.view.name == view_name]
        elif view_name:
            return self._view_fields(view_name, show_hidden, expand_dimension_groups)
        else:
            return self._explore_fields(explore_name, show_hidden, expand_dimension_groups, show_excluded)

    def _all_fields(self, show_hidden: bool, expand_dimension_groups: bool):
        return [f for v in self.views() for f in v.fields(show_hidden, expand_dimension_groups)]

    def _view_fields(
        self,
        view_name: str,
        show_hidden: bool = True,
        expand_dimension_groups: bool = False,
        explore: Explore = None,
    ):
        view = self.get_view(view_name, explore=explore)
        if not view:
            plus_explore = f" in explore {explore.name}" if explore else ""
            raise ValueError(f"Could not find a view matching the name {view_name}{plus_explore}")
        return view.fields(show_hidden, expand_dimension_groups)

    def _explore_fields(
        self, explore_name: str, show_hidden: bool, expand_dimension_groups: bool, show_excluded: bool
    ):
        explore = self.get_explore(explore_name)
        if show_excluded:
            valid_views = self.views_with_explore(explore=explore)
            return [f for v in valid_views for f in v.fields(show_hidden, expand_dimension_groups)]
        return explore.explore_fields(show_hidden, expand_dimension_groups, show_excluded)

    @functools.lru_cache(maxsize=None)
    def get_field(
        self, field_name: str, explore_name: str = None, view_name: str = None, show_excluded: bool = False
    ) -> Field:
        # Handle the case where the explore syntax is passed: explore_name.view_name.field_name
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

        fields = self.fields(
            explore_name=explore_name,
            view_name=view_name,
            expand_dimension_groups=True,
            show_excluded=show_excluded,
        )
        matching_fields = [f for f in fields if f.equal(field_name)]
        return self._matching_field_handler(matching_fields, field_name, explore_name, view_name)

    def get_explore_from_field(self, field_name: str):
        # If it's specified this is really easy
        explore_name, view_name, to_match = Field.field_name_parts(field_name)
        if explore_name is not None:
            return explore_name

        # If it's not we have to check all explores to make sure the field isn't ambiguously referenced
        all_fields_with_explore_duplicates = []
        for explore in self.explores():
            for view in self.views_with_explore(explore_name=explore.name):
                all_fields_with_explore_duplicates.extend(view.fields(expand_dimension_groups=True))

        matching_fields = [f for f in all_fields_with_explore_duplicates if f.alias() == to_match]
        if view_name:
            matching_fields = [f for f in matching_fields if f.view.name == view_name]
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
            err_msg += ", please check that this field exists AND that you have access to it. \n\n"
            err_msg += "If this is a dimension group specify the group parameter, if not already specified, "
            err_msg += "for example, with a dimension group named 'order' with timeframes: [raw, date, month]"
            err_msg += " specify 'order_raw' or 'order_date' or 'order_month'"
            raise AccessDeniedOrDoesNotExistException(err_msg, object_name=field_name, object_type="field")

    def resolve_dbt_ref(self, ref_name: str, view_name: str = None):
        if not self.manifest_exists:
            raise ValueError(
                f"Could not find a dbt project co-located with this "
                f"project to resolve the dbt ref('{ref_name}') in view {view_name}"
            )
        return self.manifest.resolve_name(ref_name)

    @staticmethod
    def _fully_qualified_name(field: Field):
        name = f"{field.view.name}.{field.name}"
        if field.view.explore:
            name = f"{field.view.explore.name}.{name}"
        return name
