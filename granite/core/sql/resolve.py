import sqlparse
from sqlparse.tokens import Name

from granite.core.model.project import Project


class SQLResolverBase:
    def __init__(
        self,
        metrics: list,
        dimensions: list = [],
        where: str = [],
        having: str = [],
        order_by: str = [],
        project: Project = None,
    ):
        self.project = project
        self.metrics = metrics
        self.dimensions = dimensions
        self.where = where
        self.having = having
        self.order_by = order_by
        self.validate()

    def validate(self):
        # Metrics exceptions:
        #   they are coming from different explores (they're not joinable)
        #   they are coming from same explore but are incompatible (they're not joinable)

        metrics = [self.project.get_field(metric_name) for metric_name in self.metrics]
        if len(set(field.view.name for field in metrics)) > 1:
            raise ValueError("cannot have metrics from more than one view")

    def get_field_lookup(self, field_names: list):
        for name in field_names:
            field = self.project.get_field(name)
            field
        return {}

    def get_field_names(self):
        field_names = self.metrics + self.dimensions
        field_names += self.parse_identifiers_from_clause(self.where)
        field_names += self.parse_identifiers_from_clause(self.having)
        field_names += self.parse_identifiers_from_clause(self.order_by)
        return field_names

    @staticmethod
    def parse_identifiers_from_clause(clause: str):
        generator = sqlparse.parse(clause)[0].flatten()
        return [str(token) for token in generator if token.ttype == Name]


class SQLResolverByQuery(SQLResolverBase):
    pass


class SQLResolverRawQuery(SQLResolverBase):
    pass
