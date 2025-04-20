import os

from .project_reader_base import ProjectReaderBase


class MetricsLayerProjectReader(ProjectReaderBase):
    def load(self) -> tuple:
        models, views, dashboards, topics = [], [], [], []

        model_folders = self.get_folders("model-paths")
        view_folders = self.get_folders("view-paths")
        dashboard_folders = self.get_folders("dashboard-paths", raise_errors=False)
        topic_folders = self.get_folders("topic-paths", raise_errors=False)
        all_folders = model_folders + view_folders + dashboard_folders + topic_folders

        file_names = self.search_for_yaml_files(all_folders)

        for fn in file_names:
            yaml_dict = self.read_yaml_file(fn)
            yaml_dict["_file_path"] = os.path.relpath(fn, start=self.repo.folder)

            # Handle keyerror
            if "type" not in yaml_dict and "zenlytic_project" not in fn:
                print(f"WARNING: file {fn} is missing a type")

            yaml_type = yaml_dict.get("type")

            if yaml_type == "model":
                models.append(yaml_dict)
            elif yaml_type == "view":
                views.append(yaml_dict)
            elif yaml_type == "dashboard":
                dashboards.append(yaml_dict)
            elif yaml_type == "topic":
                topics.append(yaml_dict)
            elif yaml_type:
                print(
                    f"WARNING: Unknown file type '{yaml_type}' options are 'model', 'view', 'dashboard', "
                    "or 'topic'"
                )

        return models, views, dashboards, topics, []
