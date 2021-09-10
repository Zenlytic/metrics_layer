# This takes an incoming SQL statement and converts the MQL() section of it
# into our internal format, asks the main query method to resolve the sql
# for that configuration then replaces the MQL() section of the original string
# with the correct SQL

from granite.core.model.project import Project


class Converter:
    """
    Syntax here is:
    MQL(
        metric_name
        BY
        dimension
        WHERE
        condition
        HAVING
        having_condition
        ORDER BY
        metric_name
    )

    for ordinary queries, and:
    MQL(
        RAW
        metric_name, metric_name
        FEATURES
        dimension, dimension
        WHERE
        condition
        ORDER BY
        metric_name
    )

    """

    def __init__(self, project: Project):
        self.reserved_keywords = ["BY", "RAW", "FEATURES"]
        self.project = project

    def convert(self, sql: str):
        raise NotImplementedError
