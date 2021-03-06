import functools
import hashlib
import json
from collections import Counter

from metrics_layer.core.exceptions import AccessDeniedOrDoesNotExistException, QueryError
from .dashboard import Dashboard
from .join_graph import JoinGraph
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
        self._timezone = None
        self._join_graph = None

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

    def set_timezone(self, timezone: str):
        self._timezone = timezone

    @property
    def timezone(self):
        if self._timezone:
            return self._timezone
        for m in self.models():
            if m.timezone:
                return m.timezone
        return None

    @property
    def join_graph(self):
        if self._join_graph is None:
            graph = JoinGraph(self)
            graph.build()
            self._join_graph = graph
        return self._join_graph

    def validate(self):
        all_errors = []

        for model in self.models():
            all_errors.extend(model.collect_errors())

        all_errors.extend(self.join_graph.collect_errors())
        for view in self.views():
            try:
                view.sql_table_name
            except QueryError as e:
                all_errors.append(str(e))

            referenced_fields = view.referenced_fields()
            view_errors = view.collect_errors()
            all_errors.extend(
                [
                    f"Could not locate reference {field} in view {view.name}"
                    for field in referenced_fields
                    if isinstance(field, str)
                ]
            )
            all_errors.extend(view_errors)

        for dashboard in self.dashboards():
            errors = dashboard.collect_errors()
            all_errors.extend(errors)

        all_errors.extend(self._validate_dashboard_names())

        return list(sorted(set(all_errors), key=lambda x: all_errors.index(x)))

    def _validate_dashboard_names(self):
        # We need to make sure the unique identifiers for the dashboards are actually unique
        errors = []
        dashboard_names = [d.name for d in self.dashboards()]
        name_frequency = Counter(dashboard_names).most_common()
        for name, frequency in name_frequency:
            if frequency > 1:
                msg = f"Dashboard name {name} appears {frequency} times, make sure dashboard names are unique"
                errors.append(msg)
            else:
                break
        return errors

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
        return [Model(m, project=self) for m in self._models]

    def get_model(self, model_name: str) -> Model:
        return next((m for m in self.models() if m.name == model_name), None)

    def access_grants(self):
        return [AccessGrant(g) for m in self.models() for g in m.access_grants]

    def get_access_grant(self, grant_name: str):
        try:
            return next((ag for ag in self.access_grants() if ag.name == grant_name))
        except StopIteration:
            raise QueryError(f"Could not find the access grant {grant_name} in your project.")

    def can_access_dashboard(self, dashboard: Dashboard):
        return self._can_access_object(dashboard)

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

    def _all_views(self, model):
        views = []
        for v in self._views:
            view = View({**v, "model": model}, project=self)
            if self.can_access_view(view):
                views.append(view)
        return views

    def views(self, model: Model = None) -> list:
        return self._all_views(model)

    def get_view(self, view_name: str, model: Model = None) -> View:
        try:
            return next((v for v in self.views(model=model) if v.name == view_name))
        except StopIteration:
            raise AccessDeniedOrDoesNotExistException(
                f"Could not find or you do not have access to view {view_name}",
                object_name=view_name,
                object_type="view",
            )

    def sets(self, view_name: str = None):
        if view_name:
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
        view_name: str = None,
        show_hidden: bool = True,
        expand_dimension_groups: bool = False,
        model: Model = None,
    ) -> list:
        if view_name is None:
            return self._all_fields(show_hidden, expand_dimension_groups, model)
        else:
            return self._view_fields(view_name, show_hidden, expand_dimension_groups, model)

    def _all_fields(self, show_hidden: bool, expand_dimension_groups: bool, model: Model):
        return [f for v in self.views(model=model) for f in v.fields(show_hidden, expand_dimension_groups)]

    def _view_fields(
        self,
        view_name: str,
        show_hidden: bool = True,
        expand_dimension_groups: bool = False,
        model: Model = None,
    ):
        view = self.get_view(view_name, model=model)
        if not view:
            plus_model = f" in model {model.name}" if model else ""
            raise QueryError(f"Could not find a view matching the name {view_name}{plus_model}")
        return view.fields(show_hidden, expand_dimension_groups)

    def joinable_fields(self, field_list: list, expand_dimension_groups: bool = False):
        join_graph = None
        join_graph_options = []
        for field in field_list:
            if field.is_merged_result:
                join_graph = field.join_graphs()[0]
            join_graph_options.extend(field.join_graphs())

        # If we have an option for a non-merged result, we'll take that unless it's required by another field
        most_common = Counter(join_graph_options).most_common()
        most_common = [(j[0], j[-1] - 1 if "merged_result" in j[0] else j[-1]) for j in most_common]

        if join_graph is None:
            join_graph = sorted(most_common, key=lambda x: -1 * x[1])[0][0]

        all_fields = self.fields(expand_dimension_groups=expand_dimension_groups)
        return [f for f in all_fields if join_graph in f.join_graphs()]

    @functools.lru_cache(maxsize=None)
    def get_field(self, field_name: str, view_name: str = None, model: Model = None) -> Field:
        field_name, view_name = self._parse_field_and_view_name(field_name, view_name)

        fields = self.fields(view_name=view_name, expand_dimension_groups=True, model=model)
        matching_fields = [f for f in fields if f.equal(field_name)]
        return self._matching_field_handler(matching_fields, field_name, view_name)

    def get_field_by_name(self, field_name: str, view_name: str = None, model: Model = None):
        field_name, view_name = self._parse_field_and_view_name(field_name, view_name)
        fields = self.fields(view_name=view_name, expand_dimension_groups=False, model=model)
        matching_fields = [f for f in fields if f.name == field_name]
        return self._matching_field_handler(matching_fields, field_name, view_name)

    def _parse_field_and_view_name(self, field_name: str, view_name: str):
        # Handle the case where the view syntax is passed: view_name.field_name
        if "." in field_name:
            _, specified_view_name, field_name = Field.field_name_parts(field_name)
            if view_name and specified_view_name != view_name:
                raise QueryError(
                    f"You specified two different view names {specified_view_name} and {view_name}"
                )
            view_name = specified_view_name
        return field_name.lower(), view_name

    def _matching_field_handler(self, matching_fields: list, field_name: str, view_name: str = None):
        if len(matching_fields) == 1:
            return matching_fields[0]

        elif len(matching_fields) > 1:
            matching_names = [f.id() for f in matching_fields]
            view_text = f", in view {view_name}" if view_name else ""
            err_msg = (
                f"Multiple fields found for the name {field_name}{view_text}"
                f" - those fields were {matching_names} \n\nPlease specify a "
                "view name like this: 'view_name.field_name' "
                "\n\nor change the names of the fields to ensure uniqueness"
            )
            raise QueryError(err_msg)
        elif field_name == "count" and view_name:
            definition = {"type": "count", "name": "count", "field_type": "measure"}
            return Field(definition, view=self.get_view(view_name))
        else:
            err_msg = f"Field {field_name} not found"
            if view_name:
                err_msg += f" in view {view_name}"
            err_msg += ", please check that this field exists AND that you have access to it. \n\n"
            err_msg += "If this is a dimension group specify the group parameter, if not already specified, "
            err_msg += "for example, with a dimension group named 'order' with timeframes: [raw, date, month]"
            err_msg += " specify 'order_raw' or 'order_date' or 'order_month'"
            raise AccessDeniedOrDoesNotExistException(err_msg, object_name=field_name, object_type="field")

    def resolve_dbt_ref(self, ref_name: str, view_name: str = None):
        if not self.manifest_exists:
            raise QueryError(
                f"Could not find a dbt project co-located with this "
                f"project to resolve the dbt ref('{ref_name}') in view {view_name}"
            )
        return self.manifest.resolve_name(ref_name)

    @staticmethod
    def deduplicate_fields(field_list: list):
        result, running_field_list = [], []
        for field in field_list:
            if field.id() not in running_field_list:
                running_field_list.append(field.id())
                result.append(field)
        return result
