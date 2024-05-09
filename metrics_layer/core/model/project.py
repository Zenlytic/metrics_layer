import functools
import json
from collections import Counter
from contextlib import contextmanager
from typing import List, Union

from metrics_layer.core.exceptions import (
    AccessDeniedOrDoesNotExistException,
    QueryError,
)

from .dashboard import Dashboard
from .field import Field
from .join_graph import JoinGraph
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
        looker_env: Union[None, str] = None,
        connection_lookup: dict = {},
        manifest=None,
        commit_hash=None,
    ):
        self._models = models
        self._views = self._handle_join_as_duplication(views)
        self._dashboards = dashboards
        self.looker_env = looker_env
        self.connection_lookup = connection_lookup
        self.manifest = manifest
        self.manifest_exists = manifest and manifest.exists()
        self._user = None
        self._connection_schema = None
        self._timezone = None
        self._required_access_filter_user_attributes = []
        self._join_graph = None
        self.commit_hash = commit_hash

    def __repr__(self):
        text = "models" if len(self._models) != 1 else "model"
        return f"<Project {len(self._models)} {text} user={self._user}>"

    def __hash__(self):
        user_str = "" if not self._user else json.dumps(self._user, sort_keys=True)
        return self._content_hash + hash(user_str)

    def refresh_cache(self):
        # Clear LRU Caches
        self.fields.cache_clear()
        self.get_field.cache_clear()
        self.get_field_by_name.cache_clear()
        self.get_field_by_tag.cache_clear()

        # Clear physical caches
        self._join_graph = None

    @functools.cached_property
    def _content_hash(self):
        model_str = json.dumps(self._models, sort_keys=True)
        view_str = json.dumps(self._views, sort_keys=True)
        dash_str = json.dumps(self._dashboards, sort_keys=True)
        conn_str = json.dumps(self.connection_lookup, sort_keys=True)
        string_to_hash = model_str + view_str + dash_str + conn_str + str(self.looker_env)
        return hash(string_to_hash)

    def set_user(self, user: dict):
        self._user = user

    def set_connection_schema(self, schema: str):
        self._connection_schema = schema

    def set_timezone(self, timezone: str):
        self._timezone = timezone

    def set_required_access_filter_user_attributes(self, user_attribute_names: List[str]):
        if not isinstance(user_attribute_names, list):
            raise QueryError("The required_access_filter_user_attributes must be a list of strings")
        self._required_access_filter_user_attributes = user_attribute_names

    def add_field(self, field: dict, view_name: str, refresh_cache: bool = True):
        view = next((v for v in self._views if v["name"] == view_name), None)
        if view is None:
            raise AccessDeniedOrDoesNotExistException(
                f"Could not find a view matching the name {view_name}",
                object_name=view_name,
                object_type="view",
            )
        # If the field already exists, then do not add it
        if not any(f["name"].lower() == field["name"].lower() for f in view["fields"]):
            view["fields"].append(field)
        if refresh_cache:
            self.refresh_cache()

    def remove_field(self, field_name: str, view_name: str, refresh_cache: bool = True):
        view = next((v for v in self._views if v["name"] == view_name), None)
        if view is None:
            raise AccessDeniedOrDoesNotExistException(
                f"Could not find a view matching the name {view_name}",
                object_name=view_name,
                object_type="view",
            )
        view["fields"] = [f for f in view["fields"] if f["name"] != field_name]
        if refresh_cache:
            self.refresh_cache()

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

    def _handle_join_as_duplication(self, views: list):
        join_as_to_create = {}
        copied_views = json.loads(json.dumps(views))
        for v in copied_views:
            for identifier in v.get("identifiers", []):
                if "join_as" in identifier and identifier["type"] == "primary":
                    # To assign the join ONLY to the new view, we need to
                    # remove the identifier from the original view
                    v["identifiers"] = [i for i in v["identifiers"] if i["name"] != identifier["name"]]

                    # And we need to remove the join_as statement from the
                    # identifier when we add it to the new view
                    identifier_to_add = {**identifier}
                    identifier_to_add.pop("join_as")
                    if identifier["join_as"] not in join_as_to_create:
                        view_args = {
                            "identifiers": [identifier_to_add],
                            "fields": json.loads(json.dumps(v.get("fields", []))),
                        }
                        if "join_as_label" in identifier:
                            view_args["label"] = identifier["join_as_label"]

                        if "join_as_field_prefix" in identifier:
                            view_args["field_prefix"] = identifier["join_as_field_prefix"]
                        elif "join_as_label" in identifier:
                            view_args["field_prefix"] = identifier["join_as_label"]
                        else:
                            view_args["field_prefix"] = identifier["join_as"].replace("_", " ").title()

                        include_metrics = identifier.get("include_metrics", False)
                        if not include_metrics:
                            view_args["fields"] = [
                                f for f in view_args["fields"] if f.get("field_type") != "measure"
                            ]

                        join_as_to_create[identifier["join_as"]] = {**v, **view_args}

                    else:
                        if join_as_to_create[identifier["join_as"]]["name"] != v["name"]:
                            raise QueryError(
                                "You cannot have join_as with identical names on different views. "
                                "Please rename your join_as statement on one of your views."
                            )

        for view_name, view in join_as_to_create.items():
            copied_views.append({**view, "name": view_name})

        return copied_views

    @contextmanager
    def replace_objects(self, replaced_objects: list):
        replaced_views, replaced_models, replaced_dashboards = [], [], []
        for dict_obj in replaced_objects:
            if isinstance(dict_obj, dict):
                if dict_obj.get("type") == "view":
                    replaced_views.append(dict_obj)
                elif dict_obj.get("type") == "model":
                    replaced_models.append(dict_obj)
                elif dict_obj.get("type") == "dashboard":
                    replaced_dashboards.append(dict_obj)
                else:
                    # We cannot use the object if it is not a view, model or dashboard
                    pass

        # Replace model files
        replaced_model_names = set([m["name"] for m in replaced_models])
        unchanged_models = [m for m in self._models if m["name"] not in replaced_model_names]
        current_models = json.loads(json.dumps(self._models))

        # Replace view files
        replaced_view_names = set([v["name"] for v in replaced_views])
        unchanged_views = [v for v in self._views if v["name"] not in replaced_view_names]
        current_views = json.loads(json.dumps(self._views))

        # Replace dashboard files
        replaced_dashboard_names = set([d["name"] for d in replaced_dashboards])
        unchanged_dashboards = [d for d in self._dashboards if d["name"] not in replaced_dashboard_names]
        current_dashboards = json.loads(json.dumps(self._dashboards))

        try:
            self._models = unchanged_models + replaced_models
            self._views = unchanged_views + replaced_views
            self._dashboards = unchanged_dashboards + replaced_dashboards
            self.refresh_cache()
            yield
        finally:
            self._dashboards = current_dashboards
            self._views = current_views
            self._models = current_models
            self.refresh_cache()

    def validate_with_replaced_objects(self, replaced_objects: list):
        with self.replace_objects(replaced_objects):
            return self.validate()

    def _error(self, error: str, extra: dict = {}):
        # For project level errors we cannot attribute a line or column
        return {**extra, "message": error, "line": None, "column": None}

    def validate(self):
        all_errors = []
        for model in self.models():
            try:
                all_errors.extend(model.collect_errors())
            except QueryError as e:
                # If we have an error building the model, we cannot continue
                return [self._error(str(e))]

        try:
            all_errors.extend(self.join_graph.collect_errors())
        except QueryError as e:
            # If we have an error building the graph, we cannot continue
            # and no other errors will be relevant until this is fixed
            return [self._error(str(e))]

        for join_graph in self.join_graph.list_join_graphs():
            try:
                self.get_field_by_tag(tag_name="customer", join_graphs=(join_graph,))
            except QueryError as e:
                error_text = str(e).replace(" name ", " tag ").split("\n")[0]
                error_text += '. Only one field can have the tag "customer" per joinable graph.'
                all_errors.append(self._error(error_text))
            except Exception:
                pass

        for view in self.views():
            if len(self._required_access_filter_user_attributes) > 0:
                for user_attribute_name in self._required_access_filter_user_attributes:
                    if not view.access_filters:
                        all_errors.append(
                            self._error(
                                (
                                    f"View {view.name} does not have any access filters, but an access filter"
                                    f" with user attribute {user_attribute_name} is required."
                                ),
                                {"view_name": view.name},
                            )
                        )
                    elif all(af["user_attribute"] != user_attribute_name for af in view.access_filters):
                        all_errors.append(
                            self._error(
                                (
                                    f"View {view.name} does not have an access filter with the required user"
                                    f" attribute {user_attribute_name}"
                                ),
                                {"view_name": view.name},
                            )
                        )

            try:
                view.sql_table_name
            except QueryError as e:
                all_errors.append(self._error(str(e) + f" in the view {view.name}", {"view_name": view.name}))
            try:
                referenced_fields = view.referenced_fields()
            except (AccessDeniedOrDoesNotExistException, QueryError) as e:
                all_errors.append(self._error(str(e) + f" in the view {view.name}", {"view_name": view.name}))

            view_errors = view.collect_errors()

            for field in referenced_fields:
                if isinstance(field, tuple):
                    if "Warning: " in field[-1]:
                        field_name = field[0].name
                        field_reference = field[-1].replace("Warning: ", "")
                        prepend = "Warning: "
                    else:
                        field_name = field[0].name
                        field_reference = field[-1]
                        prepend = ""
                    all_errors.append(
                        self._error(
                            (
                                f"{prepend}Could not locate reference {field_reference} in field"
                                f" {field_name} in view {view.name}"
                            ),
                            {"view_name": view.name, "field_name": field_name},
                        )
                    )
            all_errors.extend(view_errors)

        for dashboard in self.dashboards():
            errors = dashboard.collect_errors()
            all_errors.extend(errors)

        all_errors.extend(self._validate_dashboard_names())

        cleaned_errors, _seen = [], set([])
        for e in all_errors:
            if isinstance(e, dict) and e["message"] not in _seen:
                cleaned_errors.append(e)
                _seen.add(e["message"])

        return cleaned_errors

    def _validate_dashboard_names(self):
        # We need to make sure the unique identifiers for the dashboards are actually unique
        errors = []
        dashboard_names = [d.name for d in self.dashboards()]
        name_frequency = Counter(dashboard_names).most_common()
        for name, frequency in name_frequency:
            if frequency > 1:
                msg = f"Dashboard name {name} appears {frequency} times, make sure dashboard names are unique"
                errors.append(self._error(msg))
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
        try:
            return next((m for m in self.models() if m.name == model_name))
        except StopIteration:
            raise AccessDeniedOrDoesNotExistException(
                f"Could not find or you do not have access to model {model_name}",
                object_name=model_name,
                object_type="model",
            )

    def access_grants(self):
        return [AccessGrant(g, model=m) for m in self.models() for g in m.access_grants]

    def get_access_grant(self, grant_name: str):
        try:
            return next((ag for ag in self.access_grants() if ag.name == grant_name))
        except StopIteration:
            raise QueryError(f"Could not find the access grant {grant_name} in your project.")

    def can_access_dashboard(self, dashboard: Dashboard):
        return self._can_access_object(dashboard)

    def can_access_view(self, view: View):
        return self._can_access_object(view)

    def can_access_field(self, field):
        can_access_view = self.can_access_view(field.view)
        return self._can_access_object(field) and can_access_view

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

    def views(self, model: Union[Model, None] = None) -> list:
        return self._all_views(model)

    def get_view(self, view_name: str, model: Union[Model, None] = None) -> View:
        try:
            return next((v for v in self.views(model=model) if v.name == view_name))
        except StopIteration:
            raise AccessDeniedOrDoesNotExistException(
                f"Could not find or you do not have access to view {view_name}",
                object_name=view_name,
                object_type="view",
            )

    def sets(self, view_name: Union[str, None] = None):
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

    def get_set(self, set_name: str, view_name: Union[str, None] = None):
        if view_name:
            sets = self.sets(view_name=view_name)
        else:
            sets = self.sets()
        return next((s for s in sets if s.name == set_name), None)

    @functools.lru_cache(maxsize=None)
    def fields(
        self,
        view_name: Union[str, None] = None,
        show_hidden: bool = True,
        expand_dimension_groups: bool = False,
        model: Union[Model, None] = None,
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
        join_graph_options = set()
        for field in field_list:
            join_graph_options.update(field.join_graphs())

        all_fields = self.fields(expand_dimension_groups=expand_dimension_groups)
        field_options = [f for f in all_fields if any(j in join_graph_options for j in f.join_graphs())]
        return field_options

    @functools.lru_cache(maxsize=None)
    def get_field(self, field_name: str, view_name: str = None, model: Model = None) -> Field:
        field_name, view_name = self._parse_field_and_view_name(field_name, view_name)

        fields = self.fields(view_name=view_name, expand_dimension_groups=True, model=model)
        matching_fields = [f for f in fields if f.equal(field_name)]
        return self._matching_field_handler(matching_fields, field_name, view_name)

    def get_mapped_field(self, field_name: str, model: Model = None):
        if model.mappings:
            field_data = model.mappings.get(field_name.lower())
            if field_data:
                return {"name": field_name.lower(), **field_data}
        return None

    @functools.lru_cache(maxsize=None)
    def get_field_by_name(self, field_name: str, view_name: str = None, model: Model = None):
        field_name, view_name = self._parse_field_and_view_name(field_name, view_name)
        fields = self.fields(view_name=view_name, expand_dimension_groups=False, model=model)
        matching_fields = [f for f in fields if f.name == field_name]
        return self._matching_field_handler(matching_fields, field_name, view_name)

    @functools.lru_cache(maxsize=None)
    def get_field_by_tag(
        self, tag_name: str, view_name: str = None, join_graphs: tuple = None, model: Model = None
    ):
        tag_options = {tag_name, f"{tag_name}s"} if tag_name[-1] != "s" else {tag_name, tag_name[:-1]}
        fields = self.fields(view_name=view_name, expand_dimension_groups=True, model=model)
        matching_fields = [f for f in fields if f.tags and any(t in tag_options for t in f.tags)]
        if join_graphs:
            matching_fields = [f for f in matching_fields if any(j in f.join_graphs() for j in join_graphs)]
        return self._matching_field_handler(matching_fields, tag_name, view_name)

    def does_field_exist(self, field_name: str, view_name: str = None, model: Model = None):
        try:
            self.get_field(field_name, view_name, model)
            return True
        except AccessDeniedOrDoesNotExistException:
            return False

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
                f" - those fields were {matching_names}\n\nPlease specify a "
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

    def resolve_dbt_ref(self, ref_name: str):
        # This just returns the table name, assuming the schema will be set in the connection
        if not self.manifest_exists:
            if not self._connection_schema:
                raise QueryError(
                    "You must specify a schema in the connection to "
                    "use references without a dbt project manifest"
                )
            return f"{self._connection_schema}.{ref_name}"
        return self.manifest.resolve_name(ref_name, schema_override=self._connection_schema)

    @staticmethod
    def deduplicate_fields(field_list: list):
        result, running_field_list = [], []
        for field in field_list:
            if field.id() not in running_field_list:
                running_field_list.append(field.id())
                result.append(field)
        return result
