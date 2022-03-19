from .base import AccessDeniedOrDoesNotExistException, MetricsLayerBase
from .join import Join
from .set import Set


class Explore(MetricsLayerBase):
    def __init__(self, definition: dict = {}, project=None) -> None:
        if definition.get("from") is not None:
            definition["from_"] = definition["from"]
        elif definition.get("view_name") is not None:
            definition["from_"] = definition["view_name"]
        else:
            definition["from_"] = definition["name"]

        self.project = project
        self._view_names = []
        self.validate(definition)
        super().__init__(definition)

    @property
    def week_start_day(self):
        if self.model.week_start_day:
            return self.model.week_start_day.lower()
        return "monday"

    def validate(self, definition: dict):
        required_keys = ["name", "model", "from_"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Explore missing required key {k}")

    def printable_attributes(self):
        to_print = ["name", "type", "from", "label", "group_label", "model_name", "join_names"]
        attributes = self.to_dict()
        attributes["type"] = "explore"
        attributes["from"] = attributes["from_"]
        attributes["join_names"] = [j["name"] for j in attributes.get("joins", [])]
        return {key: attributes.get(key) for key in to_print if attributes.get(key) is not None}

    def validate_fields(self):
        errors = []
        if not self.valid_name(self.name):
            errors.append(self.name_error("explore", self.name))

        for join in self.joins():
            errors.extend(join.collect_errors())

        for view_name in self.view_names():
            try:
                view = self.project.get_view(view_name)
            except Exception:
                errors.append(f"View {view_name} cannot be found in explore {self.name}")
                continue
            try:
                view.sql_table_name
            except ValueError as e:
                errors.append(f"{str(e)} in explore {self.name}")

            referenced_fields = view.referenced_fields()
            view_errors = view.collect_errors()

            errors.extend(
                [
                    f"Could not locate reference {field} in view {view_name} in explore {self.name}"
                    for field in referenced_fields
                    if isinstance(field, str)
                ]
            )
            errors.extend(view_errors)

        return list(sorted(list(set(errors))))

    def _all_joins(self):
        joins = []
        for j in self._definition.get("joins", []):
            join = Join(j, explore=self, project=self.project)
            if self.project.can_access_join(join, explore=self):
                joins.append(join)
        return joins

    def view_names(self):
        if self._view_names:
            return self._view_names
        self._view_names = [self.from_] + [j.from_ for j in self.joins()]
        return self._view_names

    def join_names(self):
        return [self.name] + [j.name for j in self._all_joins()]

    def field_names(self):
        # This function is for the explore `fields` parameter, to resolve all the sets into field names
        if self.fields is None:
            return self.fields

        set_definition = {"name": "NA", "fields": self.fields, "explore_name": self.name}
        explore_set = Set(set_definition, project=self.project, explore=self)
        return explore_set.field_names()

    def get_join(self, join_name: str, by_view_name: bool = False):
        if by_view_name:
            return next((j for j in self.joins() if j.from_ == join_name), None)
        return next((j for j in self.joins() if j.name == join_name), None)

    def joins(self):
        output = []
        for join in self._all_joins():
            if join.is_valid():
                output.append(join)
        return output

    def explore_fields(self, show_hidden: bool, expand_dimension_groups: bool, show_excluded: bool):
        try:
            view = self.project.get_view(self.from_, explore=self)
            fields = view.fields(show_hidden, expand_dimension_groups)
        except AccessDeniedOrDoesNotExistException:
            fields = []

        for join in self.joins():
            fields.extend(join.join_fields(show_hidden, expand_dimension_groups, show_excluded))

        explore_field_names = self.field_names()
        if explore_field_names and not show_excluded:
            return [f for f in fields if f.id(view_only=True) in explore_field_names]
        return fields
