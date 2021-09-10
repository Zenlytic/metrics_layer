import re

from .base import GraniteBase
from .field import Field


class View(GraniteBase):
    def __init__(self, definition: dict = {}) -> None:
        if "sql_table_name" in definition:
            definition["sql_table_name"] = self.resolve_sql_table_name(definition["sql_table_name"], "TODO")

        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "fields"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"View missing required key {k}")

        neither_references = "sql_table_name" not in definition and "derived_table" not in definition
        both_references = "sql_table_name" in definition and "derived_table" in definition

        if neither_references or both_references:
            raise ValueError(
                f"Incorrect table identifiers sql_table_name and derived_table (must have exactly one)"
            )

    def fields(self):
        return [Field({**f, "view": self}) for f in self.fields]

    def resolve_sql_table_name(self, sql_table_name: str, looker_env: str):
        start_cond, end_cond = "-- if", "--"
        if start_cond in sql_table_name:
            # Find the condition that is chosen in the looker env
            conditions = re.findall(f"{start_cond}([^{end_cond}]*){end_cond}", sql_table_name)
            condition = next((cond for cond in conditions if cond.strip() == looker_env))
            full_phrase = start_cond + condition + end_cond

            # Use regex to extract the value associated with the condition
            searchable_sql_table_name = sql_table_name.replace("\n", "")
            everything_between = f"{full_phrase}([^{end_cond}]*){end_cond}"
            everything_after = f"(?<={full_phrase}).*"
            result = re.search(everything_between, searchable_sql_table_name)
            if result:
                return result.group().replace(end_cond, "").strip()

            result = re.search(everything_after, searchable_sql_table_name)
            return result.group().strip()

        return sql_table_name


class Set(GraniteBase):
    def __init__(self, definition: dict = {}, view: View = None) -> None:
        self.validate(definition)
        self.view = view
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "fields"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Set missing required key {k}")

    def fields(self):
        return [Field({**f, "view": self.view}) for f in self.fields]
