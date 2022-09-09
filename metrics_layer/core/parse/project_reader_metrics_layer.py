from .project_reader_base import ProjectReaderBase


class MetricsLayerProjectReader(ProjectReaderBase):
    def load(self) -> None:
        models, views, dashboards = [], [], []
        self.has_dbt_project = self.dbt_project is not None
        if self.has_dbt_project:
            dbt_path = self.repo.dbt_path if self.repo.dbt_path else self.repo.folder
            self.generate_manifest_json(dbt_path, self.profiles_dir)
            self.manifest = self.load_manifest_json()

        model_folders = self.get_folders("model-paths")
        view_folders = self.get_folders("view-paths")
        dashboard_folders = self.get_folders("dashboard-paths", raise_errors=False)
        all_folders = model_folders + view_folders + dashboard_folders

        file_names = self.search_for_yaml_files(all_folders)

        for fn in file_names:
            yaml_dict = self.read_yaml_file(fn)

            # Handle keyerror
            if "type" not in yaml_dict:
                print(f"WARNING: file {fn} is missing a type")

            yaml_type = yaml_dict.get("type")

            if yaml_type == "model":
                models.append(yaml_dict)
            elif yaml_type == "view":
                views.append(yaml_dict)
            elif yaml_type == "dashboard":
                dashboards.append(yaml_dict)
            elif yaml_type:
                print(f"WARNING: Unknown file type '{yaml_type}' options are 'model', 'view', or 'dashboard'")

        return models, views, dashboards

    def search_for_yaml_files(self, folders: list):
        return self.repo.search("*.yml", folders) + self.repo.search("*.yaml", folders)

    def get_folders(self, key: str, raise_errors: bool = True):
        if not self.zenlytic_project:
            return []

        if key in self.zenlytic_project:
            return self.zenlytic_project[key]
        elif raise_errors:
            raise KeyError(
                f"Missing required key '{key}' in zenlytic_project.yml \n"
                "Learn more about setting these keys here: https://docs.zenlytic.com"
            )
        return []
