import re

from .base import GraniteBase
from .field import Field


class View(GraniteBase):
    def __init__(self, definition: dict = {}, project=None) -> None:
        if "sql_table_name" in definition:
            definition["sql_table_name"] = self.resolve_sql_table_name(
                definition["sql_table_name"], project.looker_env
            )

        self.project = project
        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["name", "fields"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"View missing required key {k}")

    @property
    def primary_key(self):
        return next((f for f in self.fields() if f.primary_key == "yes"), None)

    def fields(self, exclude_hidden: bool = False) -> list:
        all_fields = [Field(f, view=self) for f in self._definition.get("fields", [])]
        if exclude_hidden:
            return [field for field in all_fields if field.hidden == "yes"]
        return all_fields

    def resolve_sql_table_name(self, sql_table_name: str, looker_env: str):
        start_cond, end_cond = "-- if", "--"
        if start_cond in sql_table_name:
            # Find the condition that is chosen in the looker env
            conditions = re.findall(f"{start_cond}([^{end_cond}]*){end_cond}", sql_table_name)
            try:
                condition = next((cond for cond in conditions if cond.strip() == looker_env))
            except StopIteration:
                raise ValueError(
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
