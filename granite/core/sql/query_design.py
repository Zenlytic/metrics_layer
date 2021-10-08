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
        required_views = list(set([v for field in fields_in_query for v in field.required_views()]))
        for view_name in sorted(required_views):
            join_already_added = any(view_name == j.name for j in joins_needed_for_query)
            if not join_already_added and view_name != self.explore.from_:
                joins_needed_for_query.append(self.explore.get_join(view_name))

        print(joins_needed_for_query)
        final_joins_needed = []
        for join in joins_needed_for_query:
            required_views = join.required_views()
            for view_name in required_views:
                join_already_added = any(view_name == j.name for j in final_joins_needed)
                if not join_already_added and view_name != self.explore.from_:
                    final_joins_needed.append(self.explore.get_join(view_name))
        print(joins_needed_for_query)
        return final_joins_needed

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
