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
        required_views = list(set([v for field in fields_in_query for v in field.required_views()]))

        joins_needed_for_query = []
        for view_name in reversed(sorted(required_views)):
            joins_needed_for_query.extend(self._find_needed_joins(view_name, joins_needed_for_query))
        return list(reversed(joins_needed_for_query))

    def _find_needed_joins(self, view_name: str, joins_already_added: list):
        joins_to_add = []

        join_already_added = any(view_name == j.name for j in joins_already_added)
        if not join_already_added and view_name != self.explore.from_:
            join = self.explore.get_join(view_name)
            joins_to_add.append(join)
            for view_name in join.required_views():
                joins_to_add.extend(self._find_needed_joins(view_name, joins_already_added + [join]))
        return joins_to_add

    def get_view(self, name: str) -> GraniteBase:
        try:
            return next(t for t in self.views() if t.name == name)
        except StopIteration:
            raise ParseError(f"View {name} not found in explore {self.explore.name}")

    def get_join(self, name: str) -> GraniteBase:
        return next(j for j in self.joins() if j.name == name)

    def get_field(self, field_name: str, view_name: str = None) -> GraniteBase:
        return self.project.get_field(field_name, view_name=view_name, explore_name=self.explore.name)

    @property
    def base_view_name(self):
        return self.explore.from_
