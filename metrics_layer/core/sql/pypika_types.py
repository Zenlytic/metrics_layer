from pypika import Criterion


class LiteralValueCriterion(Criterion):
    def __init__(self, sql_query: str, alias: str = None) -> None:
        """A wrapper for a literal value criterion which is a string of valid sql"""
        super().__init__(alias)
        self.sql_query = sql_query

    def get_sql(self, **kwargs):
        return self.sql_query
