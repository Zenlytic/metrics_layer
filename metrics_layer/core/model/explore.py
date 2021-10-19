from .base import MetricsLayerBase
from .join import Join


class Explore(MetricsLayerBase):
    def __init__(self, definition: dict = {}, project=None) -> None:
        if definition.get("from") is not None:
            definition["from_"] = definition["from"]
        elif definition.get("view_name") is not None:
            definition["from_"] = definition["view_name"]
        else:
            definition["from_"] = definition["name"]

        self.project = project
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

    def view_names(self):
        return [self.from_] + [j.name for j in self.joins()]

    def get_join(self, join_name: str):
        return next((j for j in self.joins() if j.name == join_name), None)

    def joins(self):
        output = []
        for j in self._definition.get("joins", []):
            join = Join(j, explore=self, project=self.project)
            if join.is_valid():
                output.append(join)
        return output
