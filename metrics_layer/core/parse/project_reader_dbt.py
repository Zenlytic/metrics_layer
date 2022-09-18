import os
from collections import defaultdict, Counter

from .github_repo import BaseRepo
from .project_reader_base import ProjectReaderBase


class dbtProjectReader(ProjectReaderBase):
    def load(self) -> None:
        self.project_name = self.dbt_project["name"]

        self.generate_manifest_json(self.repo.folder, self.profiles_dir)
        self.manifest = self.load_manifest_json()

        dbt_profile_name = self.dbt_project.get("profile", self.project_name)
        if self.zenlytic_project:
            self.profile_name = self.zenlytic_project.get("profile", dbt_profile_name)
        else:
            self.profile_name = dbt_profile_name

        models, views = self.parse_dbt_manifest(self.manifest)
        if self.zenlytic_project:
            paths = self.zenlytic_project.get("dashboard-paths", [])
            dashboards = self._load_dbt_dashboards([os.path.join(self.repo.folder, dp) for dp in paths])
        else:
            dashboards = []

        return models, views, dashboards

    def parse_dbt_manifest(self, manifest: dict):
        views = self._load_dbt_views(manifest)
        models = self._load_dbt_models()
        return models, views

    def _load_dbt_dashboards(self, dashboard_folders: list):
        if not dashboard_folders:
            return []

        dashboards = []
        for folder in dashboard_folders:
            dashboards.extend(self._load_dashboards_from_folder(folder))

        return dashboards

    def _load_dashboards_from_folder(self, dashboard_folder: str):
        file_names = BaseRepo.glob_search(dashboard_folder, "*.yml")
        file_names += BaseRepo.glob_search(dashboard_folder, "*.yaml")

        dashboards = []
        for fn in file_names:
            yaml_dict = self.read_yaml_file(fn)

            # Handle keyerror
            if "type" not in yaml_dict:
                print(f"WARNING: file {fn} is missing a type")

            if yaml_dict.get("type") == "dashboard":
                dashboards.append(yaml_dict)
        return dashboards

    def _load_dbt_models(self):
        model = {"version": 1, "type": "model", "name": self.project_name, "connection": self.profile_name}
        return [model]

    def _load_dbt_views(self, manifest: dict):
        metrics = [self._make_dbt_metric(m) for m in manifest["metrics"].values()]
        view_keys = [k for k in manifest["nodes"].keys() if "model." in k]

        views = []
        for view_key in view_keys:
            view_raw = manifest["nodes"][view_key]
            view_metrics = [m for m in metrics if view_raw["name"] == m.get("model")]
            view = self._make_dbt_view(view_raw, view_metrics)
            views.append(view)

        return views

    def _make_dbt_view(self, view: dict, view_metrics: list):
        dimension_group_dict, metric_timestamps = defaultdict(set), []
        for m in view_metrics:
            dimension_group_dict[m["timestamp"]] |= set(m["time_grains"])
            metric_timestamps.append(m["timestamp"])

        dimension_groups = []
        for timestamp, time_grains in dimension_group_dict.items():
            matching_column = view.get("columns", {}).get(timestamp, {})
            dimension_groups.append(self._make_dbt_dimension_group(timestamp, time_grains, matching_column))

        metrics = []
        for m in view_metrics:
            m.pop("model", None)
            m.pop("timestamp", None)
            m.pop("time_grains", None)
            metrics.append(m)

        meta = view["meta"] if view["meta"] != {} else view.get("config", {}).get("meta", {})
        dimension_group_names = [d["name"] for d in dimension_groups]
        dimensions = [
            self._make_dbt_dimension(d)
            for d in view.get("columns", {}).values()
            if d["name"] not in dimension_group_names
        ]
        default_date = self._dbt_default_date(metric_timestamps)
        view_dict = {
            "version": 1,
            "type": "view",
            "name": view["name"],
            "model_name": self.profile_name,
            "description": view.get("description"),
            "row_label": meta.get("row_label"),
            "sql_table_name": f"ref('{view['name']}')",
            "default_date": default_date,
            "fields": dimensions + dimension_groups + metrics,
            **meta,
        }
        return view_dict

    @staticmethod
    def _dbt_default_date(metric_timestamps: list):
        if len(metric_timestamps) > 0:
            return Counter(metric_timestamps).most_common()[0][0]
        return

    def _make_dbt_dimension_group(self, name: str, time_grains: list, matching_column: dict):
        return {
            "field_type": "dimension_group",
            "name": name,
            "type": "time",
            "timeframes": [self._convert_in_lookup(t, {"day": "date"}) for t in time_grains],
            "sql": "${TABLE}." + name,
            **matching_column.get("meta", {}),
        }

    @staticmethod
    def _make_dbt_dimension(dimension: dict):
        return {
            "field_type": "dimension",
            "name": dimension["name"],
            "type": "string",
            "label": dimension.get("label"),
            "sql": "${TABLE}." + dimension["name"],
            "description": dimension.get("description"),
            "hidden": not dimension.get("is_dimension"),
            **dimension.get("meta", {}),
        }

    def _make_dbt_metric(self, metric: dict):
        metric_dict = {
            "name": metric["name"],
            "model": self._clean_model(metric.get("model")),
            "timestamp": metric["timestamp"],
            "time_grains": metric["time_grains"],
            "field_type": "measure",
            "type": self._convert_in_lookup(metric["type"], {"expression": "number"}),
            "label": metric.get("label"),
            "description": metric.get("description"),
            "sql": self._convert_dbt_sql(metric),
            **metric.get("meta", {}),
        }
        return metric_dict

    @staticmethod
    def _clean_model(dbt_model_name: str):
        if dbt_model_name:
            return dbt_model_name.replace("ref('", "").replace("')", "")
        return None

    @staticmethod
    def _convert_in_lookup(value: str, lookup: dict):
        if value in lookup:
            return lookup[value]
        return value

    def _convert_dbt_sql(self, metric: dict):
        if metric["type"] == "expression":
            base_metric_sql = metric["sql"]
            for metric_group in metric["metrics"]:
                for metric in metric_group:
                    base_metric_sql = base_metric_sql.replace(metric, "${" + metric + "}")
            return base_metric_sql
        else:
            metric_sql = "${TABLE}." + metric["sql"]
            return self._apply_dbt_filters(metric_sql, metric.get("filters", []))

    def _apply_dbt_filters(self, metric_sql: str, filters: list):
        if len(filters) == 0:
            return metric_sql
        core_filter = " and ".join([self._dbt_filter_to_sql(f) for f in filters])
        return f"case when {core_filter} then {metric_sql} else null end"

    @staticmethod
    def _dbt_filter_to_sql(dbt_filter: dict):
        sql = " ".join(["${" + dbt_filter["field"] + "}", dbt_filter["operator"], dbt_filter["value"]])
        return sql
