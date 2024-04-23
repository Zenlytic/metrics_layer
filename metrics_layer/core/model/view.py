import re
from typing import TYPE_CHECKING, Union

from metrics_layer.core.exceptions import (
    AccessDeniedOrDoesNotExistException,
    QueryError,
)

from .base import MetricsLayerBase, SQLReplacement
from .field import Field, ZenlyticFieldType
from .join import ZenlyticJoinRelationship, ZenlyticJoinType
from .join_graph import IdentifierTypes
from .set import Set

if TYPE_CHECKING:
    from metrics_layer.core.model.model import Model
    from metrics_layer.core.model.project import Project


class View(MetricsLayerBase, SQLReplacement):
    valid_properties = [
        "version",
        "type",
        "name",
        "model_name",
        "label",
        "description",
        "sql_table_name",
        "derived_table",
        "default_date",
        "row_label",
        "sets",
        "always_filter",
        "access_filters",
        "required_access_grants",
        "event_dimension",
        "event_name",
        "identifiers",
        "fields",
        "fields_for_analysis",
    ]
    internal_properties = ["model", "field_prefix", "_file_path"]

    def __init__(self, definition: dict, project) -> None:
        if "sets" not in definition:
            definition["sets"] = []
        self.__all_fields = None
        if "name" in definition and isinstance(definition["name"], str):
            definition["name"] = definition["name"].lower()

        self.project: Project = project
        self.validate(definition)
        super().__init__(definition)

    @property
    def sql_table_name(self):
        if "sql_table_name" in self._definition:
            return self.resolve_sql_table_name(
                str(self._definition["sql_table_name"]), self.project.looker_env
            )
        return

    @property
    def identifiers(self):
        if "identifiers" in self._definition:
            if not isinstance(self._definition["identifiers"], list):
                raise QueryError(
                    f"The identifiers property, {self._definition['identifiers']} must be a list in the view"
                    f" {self.name}"
                )
            for i in self._definition["identifiers"]:
                if not isinstance(i, dict):
                    raise QueryError(f"Identifier {i} in view {self.name} must be a dictionary")
                elif "name" not in i:
                    raise QueryError(f"Identifier in view {self.name} is missing the required name property")
                elif "type" not in i:
                    raise QueryError(
                        f"Identifier {i['name']} in view {self.name} is missing the required type property"
                    )
                elif "identifiers" in i and not isinstance(i["identifiers"], list):
                    raise QueryError(
                        f"The identifiers property, {i['identifiers']} must be a list in the identifier"
                        f" {i['name']} in view {self.name}"
                    )
                elif "identifiers" in i and isinstance(i["identifiers"], list):
                    for identifier in i["identifiers"]:
                        if not isinstance(identifier, dict):
                            raise QueryError(
                                f"Identifier {identifier} in the identifiers property of the identifier"
                                f" {i['name']} in view {self.name} must be a dictionary"
                            )
                        elif "name" not in identifier:
                            raise QueryError(
                                f"Reference {identifier} in the identifiers property of identifier"
                                f" {i['name']} in view {self.name} is missing the required name property. It"
                                " should look like - name: 'identifier_name'"
                            )
                if "type" in i and i["type"] == IdentifierTypes.join:
                    if "relationship" not in i:
                        raise QueryError(
                            f"Identifier {i['name']} in view {self.name} is missing the required relationship"
                            f" property for the type: {IdentifierTypes.join}. Options are:"
                            f" {ZenlyticJoinRelationship.options}"
                        )
                    elif i["relationship"] not in ZenlyticJoinRelationship.options:
                        raise QueryError(
                            f"Identifier {i['name']} in view {self.name} has an invalid relationship"
                            f" property. Options are: {ZenlyticJoinRelationship.options}"
                        )
                    if "sql_on" not in i:
                        raise QueryError(
                            f"Identifier {i['name']} in view {self.name} is missing the required sql_on"
                            f" property for the type: {IdentifierTypes.join}"
                        )
                    if "reference" not in i:
                        raise QueryError(
                            f"Identifier {i['name']} in view {self.name} is missing the required reference"
                            f" property for the type: {IdentifierTypes.join}"
                        )
            return self._definition["identifiers"]
        return []

    @property
    def default_date(self):
        if "default_date" in self._definition:
            return str(self._definition["default_date"])
        return None

    @property
    def event_dimension(self):
        if "event_dimension" in self._definition:
            if "." not in str(self._definition["event_dimension"]):
                return f'{self.name}.{self._definition["event_dimension"]}'
            return str(self._definition["default_date"])
        return None

    @property
    def model(self):
        try:
            if "model_name" in self._definition:
                model: Model = self.project.get_model(self._definition["model_name"])
                return model
            elif "model" in self._definition:
                model: Model = self._definition["model"]
                return model
        except AccessDeniedOrDoesNotExistException:
            raise QueryError(f"View {self.name} is missing the required model_name property")

    @property
    def week_start_day(self):
        model = self.model
        if model:
            if model and model.week_start_day:
                return model.week_start_day.lower()
        return "monday"

    def get_identifier(self, identifier_name: str):
        return next((i for i in self.identifiers if i["name"] == identifier_name), None)

    def validate(self, definition: dict):
        required_keys = ["name", "fields"]
        for k in required_keys:
            if k not in definition:
                name_str = ""
                if k != "name":
                    name_str = f" in view {definition.get('name', 'Unknown')}"
                raise QueryError(f"View missing required key {k}{name_str}")

    def printable_attributes(self):
        to_print = ["name", "type", "label", "sql_table_name", "number_of_fields"]
        attributes = self.to_dict()
        attributes["sql_table_name"] = self.sql_table_name
        attributes["number_of_fields"] = f'{len(attributes.get("fields", []))}'
        return {key: attributes.get(key) for key in to_print if attributes.get(key) is not None}

    @property
    def primary_key(self):
        return next((f for f in self.fields() if f.primary_key), None)

    def _error(self, element, error, extra: dict = {}):
        line, column = self.line_col(element)
        return {**extra, "view_name": self.name, "message": error, "line": line, "column": column}

    def collect_errors(self):
        try:
            fields = self.fields(show_hidden=True)
        except QueryError as e:
            return [self._error(self._definition.get("fields"), str(e))]

        errors = []
        try:
            model_error = (
                f"Could not find a model in the view {self.name}. Use the model_name property to specify the"
                " model."
            )
            if self.model is None:
                return [self._error(self._definition.get("model_name"), model_error)]
        except QueryError:
            return [self._error(self._definition.get("model_name"), model_error)]

        if not self.valid_name(self.name):
            errors.append(self._error(self.name, self.name_error("view", self.name)))

        if "label" in self._definition and not isinstance(self.label, str):
            errors.append(
                self._error(
                    self.label, f"The label property, {self.label} must be a string in the view {self.name}"
                )
            )

        if "description" in self._definition and not isinstance(self.description, str):
            errors.append(
                self._error(
                    self.description,
                    f"The description property, {self.description} must be a string in the view {self.name}",
                )
            )

        if "sql_table_name" in self._definition and "derived_table" in self._definition:
            errors.append(
                self._error(
                    self.sql_table_name,
                    (
                        f"Warning: View {self.name} has both sql_table_name and derived_table defined,"
                        " derived_table will be used"
                    ),
                )
            )
        if "sql_table_name" not in self._definition and "derived_table" not in self._definition:
            errors.append(
                self._error(
                    self.sql_table_name,
                    (
                        f"View {self.name} does not have a sql_table_name or derived_table defined, this view"
                        " will not work"
                    ),
                )
            )
        if "sql_table_name" in self._definition and not isinstance(self._definition["sql_table_name"], str):
            errors.append(
                self._error(
                    self.sql_table_name,
                    (
                        f"The sql_table_name property, {self._definition['sql_table_name']} must be a string"
                        f" in the view {self.name}"
                    ),
                ),
            )

        if "derived_table" in self._definition:
            derived_table = self._definition["derived_table"]
            if not isinstance(derived_table, dict):
                errors.append(
                    self._error(
                        derived_table,
                        (
                            f"The derived_table property, {derived_table} must be a dictionary in the view"
                            f" {self.name}"
                        ),
                    )
                )
            else:
                if "sql" not in derived_table:
                    errors.append(
                        self._error(
                            derived_table,
                            (
                                f"Derived table in view {self.name} is missing the sql property, this view"
                                " will not work"
                            ),
                        )
                    )
                if "sql" in derived_table and not isinstance(derived_table["sql"], str):
                    errors.append(
                        self._error(
                            derived_table,
                            (
                                f"The sql property, {derived_table['sql']} must be a string in the view"
                                f" {self.name}"
                            ),
                        )
                    )

        if "default_date" in self._definition and not isinstance(self._definition["default_date"], str):
            errors.append(
                self._error(
                    self._definition["default_date"],
                    (
                        f"The default_date property, {self.default_date} must be a string in the view"
                        f" {self.name}"
                    ),
                )
            )
        elif "default_date" in self._definition and self.default_date:
            try:
                if "." in self.default_date:
                    name = self.default_date
                else:
                    name = f"{self.name}.{self.default_date}"
                field = self.project.get_field_by_name(name)
                if field.field_type != "dimension_group" or field.type != "time":
                    errors.append(
                        self._error(
                            self._definition["default_date"],
                            (
                                f"Default date {self.default_date} is not of field_type: dimension_group and"
                                f" type: time in view {self.name}"
                            ),
                        )
                    )
            except (AccessDeniedOrDoesNotExistException, QueryError):
                errors.append(
                    self._error(
                        self._definition["default_date"],
                        (
                            f"Default date {self.default_date} in view {self.name} is not joinable to the"
                            f" view {self.name}"
                        ),
                    )
                )

        if "row_label" in self._definition and not isinstance(self.row_label, str):
            errors.append(
                self._error(
                    self._definition["row_label"],
                    f"The row_label property, {self.row_label} must be a string in the view {self.name}",
                )
            )

        if "sets" in self._definition and not isinstance(self.sets, list):
            errors.append(
                self._error(
                    self._definition["sets"],
                    f"The sets property, {self.sets} must be a list in the view {self.name}",
                )
            )
        elif "sets" in self._definition and isinstance(self.sets, list):
            for s in self.sets:
                if not isinstance(s, dict):
                    errors.append(
                        self._error(
                            self._definition["sets"], f"Set {s} in view {self.name} must be a dictionary"
                        )
                    )
                else:
                    try:
                        _set = Set({**s, "view_name": self.name}, project=self.project)
                        errors.extend(_set.collect_errors())
                    except QueryError as e:
                        errors.append(
                            self._error(self._definition["sets"], str(e) + " in the view " + self.name)
                        )

        if "always_filter" in self._definition and not isinstance(self.always_filter, list):
            errors.append(
                self._error(
                    self._definition["always_filter"],
                    (
                        f"The always_filter property, {self.always_filter} must be a list in the view"
                        f" {self.name}"
                    ),
                )
            )
        elif "always_filter" in self._definition and isinstance(self.always_filter, list):
            for f in self.always_filter:
                if not isinstance(f, dict):
                    errors.append(
                        self._error(
                            self._definition["always_filter"],
                            f"Always filter {f} in view {self.name} must be a dictionary",
                        )
                    )
                    continue

                if "field" in f and isinstance(f["field"], str) and "." not in f["field"]:
                    f["field"] = f"{self.name}.{f['field']}"
                errors.extend(
                    Field.collect_field_filter_errors(
                        f, self.project, "Always filter", "view", self.name, error_func=self._error
                    )
                )

        if self.primary_key is None:
            primary_key_error = (
                f"Warning: The view {self.name} does not have a primary key, "
                "specify one using the tag primary_key: true"
            )
            errors += [self._error(None, primary_key_error)]

        if "access_filters" in self._definition and not isinstance(self.access_filters, list):
            access_filter_error = self._error(
                self._definition["access_filters"],
                (
                    f"The view {self.name} has an access filter, {self.access_filters} that is incorrectly"
                    " specified as a when it should be a list, to specify it correctly check the"
                    " documentation for access filters at"
                    " https://docs.zenlytic.com/docs/data_modeling/access_grants#access-filters"
                ),
            )
            errors.append(access_filter_error)
        elif self.access_filters is not None and isinstance(self.access_filters, list):
            for f in self.access_filters:
                if not isinstance(f, dict):
                    errors.append(
                        self._error(
                            self._definition["access_filters"],
                            f"Access filter {f} in view {self.name} must be a dictionary",
                        )
                    )
                    continue
                if "field" not in f:
                    errors.append(
                        self._error(
                            self._definition["access_filters"],
                            f"Access filter in view {self.name} is missing the required field property",
                        )
                    )
                elif "field" in f:
                    try:
                        field = self.project.get_field(f["field"])
                    except AccessDeniedOrDoesNotExistException:
                        errors.append(
                            self._error(
                                f["field"],
                                (
                                    f"Access filter in view {self.name} is referencing a field,"
                                    f" {f['field']} that does not exist"
                                ),
                            )
                        )
                if "user_attribute" not in f:
                    errors.append(
                        self._error(
                            self._definition["access_filters"],
                            (
                                f"Access filter in view {self.name} is missing the required user_attribute"
                                " property"
                            ),
                        )
                    )
                elif "user_attribute" in f and not isinstance(f["user_attribute"], str):
                    errors.append(
                        self._error(
                            f["user_attribute"],
                            (
                                f"Access filter in view {self.name} is referencing a user_attribute,"
                                f" {f['user_attribute']} that must be a string, but is not"
                            ),
                        )
                    )

        errors.extend(
            self.collect_required_access_grant_errors(
                self._definition,
                self.project,
                f"in view {self.name}",
                f"in model {self.model_name}",
                error_func=self._error,
            )
        )
        if "event_dimension" in self._definition and not isinstance(self._definition["event_dimension"], str):
            errors.append(
                self._error(
                    self._definition["event_dimension"],
                    (
                        f"The event_dimension property, {self._definition['event_dimension']} must be a"
                        f" string in the view {self.name}"
                    ),
                )
            )
        elif "event_dimension" in self._definition and self.event_dimension:
            try:
                self.project.get_field(self.event_dimension)
            except AccessDeniedOrDoesNotExistException:
                errors.append(
                    self._error(
                        self._definition["event_dimension"],
                        (
                            f"The event_dimension property, {self.event_dimension} in the view {self.name} is"
                            " not a valid field"
                        ),
                    )
                )

        if "event_name" in self._definition and not isinstance(self.event_name, str):
            errors.append(
                self._error(
                    self._definition["event_name"],
                    f"The event_name property, {self.event_name} must be a string in the view {self.name}",
                )
            )

        if "fields_for_analysis" in self._definition and not isinstance(self.fields_for_analysis, list):
            errors.append(
                self._error(
                    self._definition["fields_for_analysis"],
                    (
                        f"The fields_for_analysis property, {self.fields_for_analysis} must be a list in the"
                        f" view {self.name}"
                    ),
                )
            )

        used_identifier_names = set()

        try:
            for i in self.identifiers:
                errors.extend(self.collect_identifier_errors(i, error_func=self._error))
                if "name" in i:
                    if i["name"] in used_identifier_names:
                        errors.append(
                            self._error(
                                i["name"], f"Duplicate identifier name {i['name']} in view {self.name}"
                            )
                        )
                    used_identifier_names.add(i["name"])
                if "identifiers" in i:
                    for identifier in i["identifiers"]:
                        if identifier.get("name") not in {_id.get("name") for _id in self.identifiers}:
                            errors.append(
                                self._error(
                                    i["name"],
                                    (
                                        f"Reference to identifier {identifier.get('name')} in the composite"
                                        f" key of identifier {i.get('name')} in view {self.name} does not"
                                        " exist"
                                    ),
                                )
                            )

        except QueryError as e:
            errors.append(self._error(self._definition.get("identifiers"), str(e)))

        primary_keys = set()
        for field in fields:
            if field.primary_key and field.field_type != ZenlyticFieldType.measure:
                primary_keys.add(field.name)
            errors.extend(field.collect_errors())

        if len(primary_keys) > 1:
            errors.append(
                self._error(
                    None,
                    (
                        f"Multiple primary keys found in view {self.name}: {', '.join(sorted(primary_keys))}."
                        " Only one primary key is allowed"
                    ),
                )
            )

        # Check for duplicate fields
        field_names = [f.name for f in fields]
        if len(field_names) != len(set(field_names)):
            errors.append(
                self._error(
                    self._definition.get("fields"),
                    (
                        f"Duplicate field names in view {self.name}:"
                        f" {', '.join(set(f for f in field_names if field_names.count(f) > 1))}"
                    ),
                )
            )

        definition_to_check = {k: v for k, v in self._definition.items() if k not in self.internal_properties}
        errors.extend(
            self.invalid_property_error(
                definition_to_check, self.valid_properties, "view", self.name, error_func=self._error
            )
        )
        return errors

    @staticmethod
    def collect_required_access_grant_errors(
        definition: dict, project, application_location: str, grant_location: str, error_func
    ):
        errors = []
        if "required_access_grants" in definition and not isinstance(
            definition["required_access_grants"], list
        ):
            errors.append(
                error_func(
                    definition["required_access_grants"],
                    (
                        f"The required_access_grants property, {definition['required_access_grants']} must be"
                        f" a list {application_location}"
                    ),
                )
            )
        elif "required_access_grants" in definition and isinstance(
            definition["required_access_grants"], list
        ):
            for f in definition["required_access_grants"]:
                if not isinstance(f, str):
                    errors.append(
                        error_func(
                            f,
                            (
                                f"The access grant reference {f} in the required_access_grants property must"
                                f" be a string {application_location}"
                            ),
                        )
                    )
                else:
                    try:
                        project.get_access_grant(f)
                    except QueryError:
                        errors.append(
                            error_func(
                                f,
                                (
                                    f"The access grant {f} in the required_access_grants property does not"
                                    f" exist {grant_location}"
                                ),
                            )
                        )
        return errors

    def collect_identifier_errors(self, identifier: dict, error_func):
        all_identifier_properties = ["name", "type"]
        errors = []

        if not isinstance(identifier["name"], str):
            errors.append(
                error_func(
                    identifier["name"],
                    (
                        f"The name property, {identifier['name']} in the identifier in view {self.name} must"
                        " be a string"
                    ),
                )
            )
            return errors
        elif not self.valid_name(identifier["name"]):
            errors.append(error_func(identifier["name"], self.name_error("identifier", identifier["name"])))

        if not isinstance(identifier["type"], str):
            errors.append(
                error_func(
                    identifier["type"],
                    (
                        f"The type property, {identifier['type']} in the identifier {identifier['name']} in"
                        f" view {self.name} must be a string"
                    ),
                )
            )
            return errors
        elif identifier["type"] not in IdentifierTypes.options:
            errors.append(
                error_func(
                    identifier["type"],
                    (
                        f"The type property, {identifier['type']} in the identifier {identifier['name']} in"
                        f" view {self.name} must be one of {IdentifierTypes.options}"
                    ),
                )
            )
            return errors

        custom_join_valid_properties = ["sql_on", "reference", "join_type", "relationship"]
        identifier_join_valid_properties = ["sql", "allowed_fanouts", "identifiers"]
        join_as_valid_properties = ["join_as", "join_as_label", "join_as_field_prefix", "include_metrics"]
        if identifier["type"] == IdentifierTypes.join:
            # Reference must reference a view that exists
            if "reference" not in identifier:
                errors.append(
                    error_func(
                        identifier["name"],
                        (
                            f"Identifier {identifier['name']} in view {self.name} is missing the required"
                            f" reference property for the {IdentifierTypes.join} type of join"
                        ),
                    )
                )
            elif not isinstance(identifier["reference"], str):
                errors.append(
                    error_func(
                        identifier["reference"],
                        (
                            f"The reference property, {identifier['reference']} in the identifier"
                            f" {identifier['name']} in view {self.name} must be a string"
                        ),
                    )
                )
            else:
                try:
                    self.project.get_view(identifier["reference"])
                except AccessDeniedOrDoesNotExistException:
                    errors.append(
                        error_func(
                            identifier["reference"],
                            (
                                f"The reference property, {identifier['reference']} in the identifier"
                                f" {identifier['name']} in view {self.name} is not a valid view"
                            ),
                        )
                    )
            # join_type must be one of the valid join types
            if "join_type" in identifier and identifier["join_type"] not in ZenlyticJoinType.options:
                errors.append(
                    error_func(
                        identifier["join_type"],
                        (
                            f"The join_type property, {identifier['join_type']} in the identifier"
                            f" {identifier['name']} in view {self.name} must be one of"
                            f" {ZenlyticJoinType.options}"
                        ),
                    )
                )

            # relationship must be one of the valid join relationships
            if (
                "relationship" in identifier
                and identifier["relationship"] not in ZenlyticJoinRelationship.options
            ):
                errors.append(
                    error_func(
                        identifier["relationship"],
                        (
                            f"The relationship property, {identifier['relationship']} in the identifier"
                            f" {identifier['name']} in view {self.name} must be one of"
                            f" {ZenlyticJoinRelationship.options}"
                        ),
                    )
                )

            # SQL on is logically verified in the join itself
            if "sql_on" in identifier and not isinstance(identifier["sql_on"], str):
                errors.append(
                    error_func(
                        identifier["sql_on"],
                        (
                            f"The sql_on property, {identifier['sql_on']} in the identifier"
                            f" {identifier['name']} in view {self.name} must be a string"
                        ),
                    )
                )
            additional_properties = custom_join_valid_properties
        else:
            if "sql" in identifier and not isinstance(identifier["sql"], str):
                errors.append(
                    error_func(
                        identifier["sql"],
                        (
                            f"The sql property, {identifier['sql']} in the identifier {identifier['name']} in"
                            f" view {self.name} must be a string"
                        ),
                    )
                )
            elif "sql" in identifier and "${" not in str(identifier["sql"]):
                errors.append(
                    error_func(
                        identifier["sql"],
                        (
                            f'Warning: Identifier {identifier["name"]} in view {self.name} is missing'
                            ' "${", are you sure you are using the reference syntax correctly?'
                        ),
                    )
                )
            # Check that referenced sql fields are valid
            elif "sql" in identifier:
                for field in self.fields_to_replace(identifier["sql"]):
                    if field != "TABLE":
                        _, view_name, column_name = Field.field_name_parts(field)
                        if view_name is None:
                            view_name = self.name
                        try:
                            self.project.get_field(f"{view_name}.{column_name}")
                        except AccessDeniedOrDoesNotExistException:
                            errors.append(
                                error_func(
                                    identifier["name"],
                                    (
                                        f"Could not find field {field} referenced in identifier"
                                        f" {identifier['name']} in view {self.name}"
                                    ),
                                )
                            )
            elif "sql" not in identifier and "identifiers" not in identifier:
                try:
                    self.project.get_field(f"{self.name}.{identifier['name']}")
                except AccessDeniedOrDoesNotExistException:
                    errors.append(
                        error_func(
                            identifier["name"],
                            (
                                f"Could not find field {identifier['name']} referenced in identifier"
                                f" {identifier['name']} in view {self.name}. Use the sql property to"
                                " reference a different field in the view"
                            ),
                        )
                    )
            elif "sql" not in identifier and "identifiers" in identifier:
                if not isinstance(identifier["identifiers"], list):
                    errors.append(
                        error_func(
                            identifier["identifiers"],
                            (
                                f"The identifiers property, {identifier['identifiers']} must be a list in the"
                                f" identifier {identifier['name']} in view {self.name}"
                            ),
                        )
                    )
                else:
                    if identifier["type"] != IdentifierTypes.primary:
                        errors.append(
                            error_func(
                                identifier["name"],
                                (
                                    f"The identifiers property on a composite key {identifier['name']}  in"
                                    f" view {self.name} is only allowed for type: primary, not type:"
                                    f" {identifier['type']}"
                                ),
                            )
                        )
                    for i in identifier["identifiers"]:
                        if not isinstance(i, dict):
                            errors.append(
                                error_func(
                                    i,
                                    (
                                        f"Identifier {i} in the identifiers property of the identifier"
                                        f" {identifier['name']} in view {self.name} must be a dictionary"
                                    ),
                                )
                            )
                        else:
                            if "name" not in i:
                                errors.append(
                                    error_func(
                                        i,
                                        (
                                            f"Identifier in view {self.name} is missing the required name"
                                            " property"
                                        ),
                                    )
                                )

            additional_properties = identifier_join_valid_properties

        if "join_as" in identifier:
            if not isinstance(identifier["join_as"], str):
                errors.append(
                    error_func(
                        identifier["join_as"],
                        (
                            f"The join_as property, {identifier['join_as']} in the identifier"
                            f" {identifier['name']} in view {self.name} must be a string"
                        ),
                    )
                )
            if "join_as_label" in identifier and not isinstance(identifier["join_as_label"], str):
                errors.append(
                    error_func(
                        identifier["join_as_label"],
                        (
                            f"The join_as_label property, {identifier['join_as_label']} in the identifier"
                            f" {identifier['name']} in view {self.name} must be a string"
                        ),
                    )
                )
            if "join_as_field_prefix" in identifier and not isinstance(
                identifier["join_as_field_prefix"], str
            ):
                errors.append(
                    error_func(
                        identifier["join_as_field_prefix"],
                        (
                            f"The join_as_field_prefix property, {identifier['join_as_field_prefix']} in the"
                            f" identifier {identifier['name']} in view {self.name} must be a string"
                        ),
                    )
                )
            if "include_metrics" in identifier and not isinstance(identifier["include_metrics"], bool):
                errors.append(
                    error_func(
                        identifier["include_metrics"],
                        (
                            f"The include_metrics property, {identifier['include_metrics']} in the identifier"
                            f" {identifier['name']} in view {self.name} must be a boolean"
                        ),
                    )
                )

        join_properties = additional_properties + all_identifier_properties + join_as_valid_properties
        for e in self.invalid_property_error(
            identifier, join_properties, "identifier", identifier["name"], error_func=error_func
        ):
            e["message"] += f" in view {self.name}"
            errors.append(e)
        return errors

    def referenced_fields(self):
        fields = self.fields(show_hidden=True)
        result = []
        for field in fields:
            all_fields = [field]
            if not field.is_merged_result:
                referenced_sql = field.get_referenced_sql_query(strings_only=False)
                if referenced_sql is not None:
                    for reference in referenced_sql:
                        if isinstance(reference, str) and field.is_personal_field:
                            all_fields.append((field, f"Warning: {reference}"))
                        elif isinstance(reference, str):
                            all_fields.append((field, reference))
                        else:
                            all_fields.append(reference)
            result.extend(all_fields)
        return result

    def fields(self, show_hidden: bool = True, expand_dimension_groups: bool = False) -> list:
        if not self.__all_fields:
            self.__all_fields = self._all_fields(expand_dimension_groups=expand_dimension_groups)
        all_fields = self.__all_fields
        if show_hidden:
            return all_fields
        return [field for field in all_fields if not field.hidden]

    def _all_fields(self, expand_dimension_groups: bool):
        fields = []
        for f in self._definition.get("fields", []):
            if self.field_prefix:
                f["label_prefix"] = self.field_prefix
            field = Field(f, view=self)
            if self.project.can_access_field(field):
                if expand_dimension_groups and field.field_type == "dimension_group":
                    if field.timeframes:
                        for timeframe in field.timeframes:
                            additional = {"hidden": True} if timeframe == "raw" else {}
                            fields.append(Field({**f, **additional, "dimension_group": timeframe}, view=self))

                    elif field.intervals:
                        for interval in field.intervals:
                            fields.append(Field({**f, "dimension_group": f"{interval}s"}, view=self))
                else:
                    fields.append(field)
        return fields

    def _field_name_to_remove(self, field_expr: str):
        # Skip the initial - sign
        field_clean_expr = field_expr[1:]
        if "." in field_clean_expr:
            view_name, field_name = field_clean_expr.split(".")
            if view_name == self.name:
                return field_name
            return None
        return field_clean_expr

    def resolve_sql_table_name(self, sql_table_name: str, looker_env: Union[str, None]):
        if "-- if" in sql_table_name:
            return self._resolve_conditional_sql_table_name(sql_table_name, looker_env)
        if "ref(" in sql_table_name:
            return self._resolve_dbt_ref_sql_table_name(sql_table_name)
        return sql_table_name

    def _resolve_dbt_ref_sql_table_name(self, sql_table_name: str):
        ref_arguments = sql_table_name[sql_table_name.find("ref(") + 4 : sql_table_name.find(")")]
        ref_value = ref_arguments.replace("'", "")
        return self.project.resolve_dbt_ref(ref_value)

    @staticmethod
    def _resolve_conditional_sql_table_name(sql_table_name: str, looker_env: Union[str, None]):
        start_cond, end_cond = "-- if", "--"

        # Find the condition that is chosen in the looker env
        conditions = re.findall(f"{start_cond}([^{end_cond}]*){end_cond}", sql_table_name)
        try:
            condition = next((cond for cond in conditions if cond.strip() == looker_env))
        except StopIteration:
            raise QueryError(
                f"""Your sql_table_name: '{sql_table_name}' contains a conditional and
                we could not match that to the conditional value you passed: {looker_env}"""
            )

        full_phrase = start_cond + condition + end_cond

        # Use regex to extract the value associated with the condition
        searchable_sql_table_name = sql_table_name.replace("\n", "")
        everything_between = f"{full_phrase}([^{end_cond}]*){end_cond}"
        everything_after = f"(?<={full_phrase}).*"
        result = re.search(everything_between, searchable_sql_table_name)
        if result:
            return result.group().replace(end_cond, "").strip()

        result = re.search(everything_after, searchable_sql_table_name)
        if result:
            return result.group().strip()
        return sql_table_name

    def list_sets(self):
        if not isinstance(self.sets, list):
            return []
        sets = []
        for s in self.sets:
            if isinstance(s, dict):
                sets.append(Set({**s, "view_name": self.name}, project=self.project))
        return sets

    def get_set(self, set_name: str):
        return next((s for s in self.list_sets() if s.name == set_name), None)
