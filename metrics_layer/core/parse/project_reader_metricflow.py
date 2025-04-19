from metrics_layer.core.exceptions import QueryError
from metrics_layer.core.model import Project
from metrics_layer.core.sql.query_design import MetricsLayerDesign
from metrics_layer.integrations.metricflow.metricflow_to_zenlytic import (
    convert_mf_project_to_zenlytic_project,
    load_mf_project,
)

from .project_reader_base import ProjectReaderBase


class MetricflowProjectReader(ProjectReaderBase):
    def load(self) -> tuple:
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
        topics = self.derive_topics(models, views)
        # models, views, dashboards, topics
        return models, views, [], topics

    def derive_topics(self, models: list, views: list) -> list:
        project = Project(models, views)
        graph = project.join_graph.graph
        sorted_components = project.join_graph._strongly_connected_components(graph)

        design = MetricsLayerDesign(
            no_group_by=False,
            query_type="SNOWFLAKE",  # this doesn't matter
            field_lookup={},
            model=None,
            project=project,
        )

        topic_views = []
        topics = {}

        for component_set in sorted_components:
            subgraph_nodes = project.join_graph._subgraph_nodes_from_components(graph, component_set)
            design._join_subgraph = project.join_graph.subgraph(subgraph_nodes)
            ordered_components = design.determine_join_order(subgraph_nodes)

            if len(ordered_components) == 0:
                ordered_views = subgraph_nodes
            else:
                ordered_views = [view for components in ordered_components for view in components]
                ordered_views = list(sorted(set(ordered_views), key=lambda x: ordered_views.index(x)))

            topic_views.append(ordered_views)

        # make sure all views have been used in a topic
        all_views = set(v.name for v in project.views())
        all_topic_views = set([topic_view for topic_view_set in topic_views for topic_view in topic_view_set])
        assert all_views == all_topic_views

        # make sure each topic has at least one view
        for tv in topic_views:
            assert len(tv) > 0

        topics = []
        for i, topic_view_list in enumerate(topic_views):
            base_view = topic_view_list[0]
            topic_data = {
                "label": f"{base_view.replace('_', ' ').title()}",
                "base_view": base_view,
                "views": {},
            }
            for view in topic_view_list[1:]:
                try:
                    design._join_subgraph = project.join_graph.subgraph([base_view, view])
                    ordered_join_components = design.determine_join_order([base_view, view])
                    if len(ordered_join_components) == 1:
                        join = project.join_graph.get_join(base_view, view)
                    else:
                        connecting_view, destination_view = ordered_join_components[-1]
                        join = project.join_graph.get_join(connecting_view, destination_view)

                    join_type = join._definition.get("type")
                    relationship = join._definition.get("relationship")
                    sql_on = join._definition.get("sql_on")

                    if join_type is not None and relationship is not None and sql_on is not None:
                        topic_data["views"][view] = {
                            "join": {
                                "join_type": join_type,
                                "relationship": relationship,
                                "sql_on": sql_on,
                            }
                        }

                except Exception as e:
                    print(f"error_getting_join_for_topic: {base_view} and {view}: {e}")

            topics.append(topic_data)

        return topics
