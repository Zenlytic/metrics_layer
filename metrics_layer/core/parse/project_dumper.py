import os
from ruamel.yaml.comments import CommentedMap, CommentedSeq

from metrics_layer.core.parse.project_reader_base import ProjectReaderBase


class ProjectDumper(ProjectReaderBase):
    def __init__(self, models: list, model_folder: str, views: list, view_folder: str):
        self.models_to_dump = models
        self._model_folder = model_folder
        self.views_to_dump = views
        self._view_folder = view_folder

    def dump(self, path: str):
        for model in self.models_to_dump:
            file_name = model["name"] + "_model.yml"
            models_folder = os.path.join(path, self._model_folder)
            if os.path.exists(models_folder):
                file_path = os.path.join(models_folder, file_name)
            else:
                file_path = os.path.join(path, file_name)
            self.dump_yaml_file(self._sort_model(model), file_path)

        for view in self.views_to_dump:
            file_name = view["name"] + "_view.yml"
            views_folder = os.path.join(path, self._view_folder)
            if os.path.exists(views_folder):
                file_path = os.path.join(views_folder, file_name)
            else:
                file_path = os.path.join(path, file_name)
            self.dump_yaml_file(self._sort_view(view), file_path)

    def _sort_view(self, view: dict):
        view_key_order = [
            "version",
            "type",
            "name",
            "model_name",
            "sql_table_name",
            "default_date",
            "row_label",
            "extends",
            "extension",
            "required_access_grants",
            "sets",
            "fields",
        ]
        extra_keys = [k for k in view.keys() if k not in view_key_order]
        new_view = CommentedMap()
        for k in view_key_order + extra_keys:
            if k in view:
                if k == "fields":
                    new_view[k] = self._sort_fields(view[k])
                else:
                    new_view[k] = view[k]
        return new_view

    def _sort_fields(self, fields: list):
        sort_key = ["dimension", "dimension_group", "measure"]
        sorted_fields = sorted(
            fields, key=lambda x: (sort_key.index(x["field_type"]), -1 if "id" in x["name"] else 0)
        )
        result_seq = CommentedSeq([self._sort_field(f) for f in sorted_fields])
        for i in range(1, len(sorted_fields)):
            result_seq.yaml_set_comment_before_after_key(i, before="\n")
        return result_seq

    def _sort_field(self, field: dict):
        field_key_order = [
            "name",
            "field_type",
            "type",
            "datatype",
            "hidden",
            "primary_key",
            "label",
            "view_label",
            "description",
            "required_access_grants",
            "value_format_name",
            "drill_fields",
            "sql_distinct_key",
            "tiers",
            "timeframes",
            "intervals",
            "sql_start",
            "sql_end",
            "sql",
            "filters",
            "extra",
        ]
        extra_keys = [k for k in field.keys() if k not in field_key_order]
        new_field = CommentedMap()
        for k in field_key_order + extra_keys:
            if k in field:
                new_field[k] = field[k]
        return new_field

    def _sort_model(self, model: dict):
        model_key_order = [
            "version",
            "type",
            "name",
            "label",
            "connection",
            "fiscal_month_offset",
            "week_start_day",
            "access_grants",
        ]
        extra_keys = [k for k in model.keys() if k not in model_key_order]
        new_model = CommentedMap()
        for k in model_key_order + extra_keys:
            if k in model:
                new_model[k] = model[k]
        return new_model
