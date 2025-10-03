import os
from collections import OrderedDict

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
            if "_file_path" in model:
                file_path = os.path.join(path, model["_file_path"])
                directory = os.path.dirname(file_path)
                if not os.path.exists(directory):
                    os.makedirs(directory)
            else:
                file_name = model["name"] + "_model.yml"
                models_folder = os.path.join(path, self._model_folder)
                if not os.path.exists(models_folder):
                    os.mkdir(models_folder)
                file_path = os.path.join(models_folder, file_name)
            self.dump_yaml_file(self._sort_model(model), file_path)

        for view in self.views_to_dump:
            if "_file_path" in view:
                file_path = os.path.join(path, view["_file_path"])
                directory = os.path.dirname(file_path)
                if not os.path.exists(directory):
                    os.makedirs(directory)
            else:
                file_name = view["name"] + "_view.yml"
                views_folder = os.path.join(path, self._view_folder)
                if not os.path.exists(views_folder):
                    os.mkdir(views_folder)
                file_path = os.path.join(views_folder, file_name)
            self.dump_yaml_file(self._sort_view(view), file_path)

    def _reorder_commented_map(self, original: CommentedMap, key_order: list[str]) -> CommentedMap:
        extra_keys = [k for k in original.keys() if k not in key_order]
        all_keys_ordered = [k for k in key_order if k in original] + extra_keys

        temp = OrderedDict()
        for k in all_keys_ordered:
            temp[k] = original[k]

        # Clear and repopulate preserves the CommentedMap object and its .ca metadata
        original.clear()
        for k, v in temp.items():
            original[k] = v

        return original

    def _sort_view(self, view: dict) -> CommentedMap:
        view_key_order = [
            "version",
            "type",
            "name",
            "label",
            "description",
            "model_name",
            "sql_table_name",
            "default_date",
            "derived_table",
            "row_label",
            "extends",
            "extension",
            "required_access_grants",
            "sets",
            "identifiers",
            "fields",
        ]

        if isinstance(view, CommentedMap):
            if "fields" in view:
                view["fields"] = self._sort_fields(view["fields"])

            return self._reorder_commented_map(view, view_key_order)
        else:
            extra_keys = [k for k in view.keys() if k not in view_key_order]
            new_view = CommentedMap()
            for k in view_key_order + extra_keys:
                if k in view:
                    if k == "fields":
                        new_view[k] = self._sort_fields(view[k])
                    else:
                        new_view[k] = view[k]
            return new_view

    def _sort_fields(self, fields: list) -> CommentedSeq:
        sort_key = ["dimension", "dimension_group", "measure"]
        sorted_fields = sorted(
            fields, key=lambda x: (sort_key.index(x["field_type"]), -1 if "id" in x["name"] else 0)
        )

        processed_fields = [self._sort_field(f) for f in sorted_fields]
        result_seq = CommentedSeq(processed_fields)

        # Only add newlines before newly created fields (original CommentedMaps already have proper spacing)
        for i in range(1, len(processed_fields)):
            if not isinstance(sorted_fields[i], CommentedMap):
                result_seq.yaml_set_comment_before_after_key(i, before="\n")

        return result_seq

    def _sort_field(self, field: dict) -> CommentedMap:
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

        if isinstance(field, CommentedMap):
            return self._reorder_commented_map(field, field_key_order)
        else:
            extra_keys = [k for k in field.keys() if k not in field_key_order]
            new_field = CommentedMap()
            for k in field_key_order + extra_keys:
                if k in field:
                    new_field[k] = field[k]
            return new_field

    def _sort_model(self, model: dict) -> CommentedMap:
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

        if isinstance(model, CommentedMap):
            return self._reorder_commented_map(model, model_key_order)
        else:
            extra_keys = [k for k in model.keys() if k not in model_key_order]
            new_model = CommentedMap()
            for k in model_key_order + extra_keys:
                if k in model:
                    new_model[k] = model[k]
            return new_model
