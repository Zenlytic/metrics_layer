from typing import List

from granite.core.model.base import GraniteBase
from granite.core.sql.query_errors import ParseError


class GraniteDesign:
    """ """

    def __init__(self, no_group_by: bool, query_type: str, field_lookup: dict, explore, project) -> None:
        self.no_group_by = no_group_by
        self.query_type = query_type
        self.field_lookup = field_lookup
        self.explore = explore
        self.project = project

    def views(self) -> List[GraniteBase]:
        return [self.project.get_view(name, explore=self.explore) for name in self.explore.view_names()]

    def joins(self) -> List[GraniteBase]:
        fields_in_query = list(self.field_lookup.values())

        joins_needed_for_query = []
        for field in fields_in_query:
            join_already_added = any(field.view.name == j.name for j in joins_needed_for_query)
            if not join_already_added and field.view.name != self.explore.from_:
                joins_needed_for_query.append(self.explore.get_join(field.view.name))
        return joins_needed_for_query

    def get_view(self, name: str) -> GraniteBase:
        return next(t for t in self.views() if t.name == name)

    def get_join(self, name: str) -> GraniteBase:
        return next(j for j in self.joins() if j.name == name)

    def find_view(self, name: str) -> GraniteBase:
        try:
            return next(t for t in self.views() if t.name == name)
        except StopIteration:
            raise ParseError(f"Table {name} not found in explore {self.explore.name}")

    def get_field(self, field_name: str, view_name: str = None) -> GraniteBase:
        if "." in field_name:
            view_name, field_name = field_name.split(".")

        if view_name is None:
            views = self.views()
        else:
            views = [self.get_view(view_name)]

        try:
            return next(f for t in views for f in t.fields(exclude_hidden=False) if f.equal(field_name))
        except StopIteration:
            raise ParseError(f"Attribute {field_name} not found in explore {self.explore.name}")

    @property
    def base_view_name(self):
        return self.explore.from_
