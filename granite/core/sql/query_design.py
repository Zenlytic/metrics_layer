from typing import List

from granite.core.model.base import GraniteBase
from granite.core.sql.query_errors import ParseError


class GraniteDesign:
    """ """

    def __init__(self, query_type: str, explore, project) -> None:
        self.query_type = query_type
        self.explore = explore
        self.project = project

    def views(self) -> List[GraniteBase]:
        return [self.project.get_view(name) for name in self.explore.view_names()]

    def joins(self) -> List[GraniteBase]:
        return self.explore.joins()

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
        if view_name is None:
            views = self.views()
        else:
            views = [self.get_view(view_name)]

        try:
            return next(f for t in views for f in t.fields(exclude_hidden=False) if f.alias() == field_name)
        except StopIteration:
            raise ParseError(f"Attribute {field_name} not found in explore {self.explore.name}")

    @property
    def base_view_name(self):
        return self.explore.from_
