from metricflow_to_zenlytic.metricflow_to_zenlytic import (
    load_mf_project,
    convert_mf_project_to_zenlytic_project,
)
from .project_reader_base import ProjectReaderBase
from metrics_layer.core.exceptions import QueryError


class MetricflowProjectReader(ProjectReaderBase):
    def load(self) -> None:
        if self.dbt_project is None:
            raise QueryError("No dbt project found")

        self.project_name = self.dbt_project["name"]

        metricflow_project = load_mf_project(self.dbt_folder)

        dbt_profile_name = self.dbt_project.get("profile", self.project_name)
        if self.zenlytic_project:
            self.profile_name = self.zenlytic_project.get("profile", dbt_profile_name)
        else:
            self.profile_name = dbt_profile_name

        models, views = convert_mf_project_to_zenlytic_project(
            metricflow_project, self.profile_name, self.profile_name
        )
        return models, views, []
