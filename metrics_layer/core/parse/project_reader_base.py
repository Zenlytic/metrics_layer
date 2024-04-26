import os

import ruamel.yaml

from .github_repo import BaseRepo


class Str(ruamel.yaml.scalarstring.ScalarString):
    __slots__ = "lc"


class ZenlyticPreservedScalarString(ruamel.yaml.scalarstring.PreservedScalarString):
    __slots__ = "lc"


class ZenlyticDoubleQuotedScalarString(ruamel.yaml.scalarstring.DoubleQuotedScalarString):
    __slots__ = "lc"


class ZenlyticSingleQuotedScalarString(ruamel.yaml.scalarstring.SingleQuotedScalarString):
    __slots__ = "lc"


# This is so we can spy on the line and column of string types for error messages
class ZenlyticConstructor(ruamel.yaml.constructor.RoundTripConstructor):
    def construct_scalar(self, node):
        # type: (Any) -> Any
        if not isinstance(node, ruamel.yaml.nodes.ScalarNode):
            raise ruamel.yaml.constructor.ConstructorError(
                None, None, "expected a scalar node, but found %s" % node.id, node.start_mark
            )

        if node.style == "|" and isinstance(node.value, str):
            ret_val = ZenlyticPreservedScalarString(node.value)
        elif bool(self._preserve_quotes) and isinstance(node.value, str):
            if node.style == "'":
                ret_val = ZenlyticSingleQuotedScalarString(node.value)
            elif node.style == '"':
                ret_val = ZenlyticDoubleQuotedScalarString(node.value)
            else:
                ret_val = Str(node.value)
        else:
            ret_val = Str(node.value)
        ret_val.lc = ruamel.yaml.comments.LineCol()
        ret_val.lc.line = node.start_mark.line
        ret_val.lc.col = node.start_mark.column
        return ret_val


class ProjectReaderBase:
    def __init__(self, repo: BaseRepo, profiles_dir: str = None):
        self.repo = repo
        self.profiles_dir = profiles_dir
        self.version = 1
        self.unloaded = True
        self.has_dbt_project = False
        self.manifest = {}
        self._models = []
        self._views = []
        self._dashboards = []

    @property
    def models(self):
        if self.unloaded:
            self.load()
        return self._models

    @property
    def views(self):
        if self.unloaded:
            self.load()
        return self._views

    @property
    def dashboards(self):
        if self.unloaded:
            self.load()
        return self._dashboards

    @property
    def zenlytic_project(self):
        return self.read_yaml_if_exists(self.zenlytic_project_path)

    @property
    def zenlytic_project_path(self):
        zenlytic_project = self.read_yaml_if_exists(os.path.join(self.repo.folder, "zenlytic_project.yml"))
        if zenlytic_project:
            return os.path.join(self.repo.folder, "zenlytic_project.yml")
        return os.path.join(self.dbt_folder, "zenlytic_project.yml")

    @property
    def dbt_project(self):
        return self.read_yaml_if_exists(os.path.join(self.dbt_folder, "dbt_project.yml"))

    @property
    def dbt_folder(self):
        return self.repo.dbt_path if self.repo.dbt_path else self.repo.folder

    @staticmethod
    def read_yaml_if_exists(file_path: str):
        if os.path.exists(file_path):
            return ProjectReaderBase.read_yaml_file(file_path)
        return None

    @staticmethod
    def read_yaml_file(path: str):
        yaml = ruamel.yaml.YAML(typ="rt")
        # HOTFIX: this somehow introduced a unicode error on multiline strings with the character
        # \u0007 (bell) in them. Commenting out the below code is a temporary fix.
        # yaml.Constructor = ZenlyticConstructor
        yaml.version = (1, 1)
        with open(path, "r") as f:
            yaml_dict = yaml.load(f)
        return yaml_dict

    @staticmethod
    def repr_str(representer, data):
        return representer.represent_str(str(data))

    @staticmethod
    def dump_yaml_file(data: dict, path: str):
        yaml = ruamel.yaml.YAML(typ="rt")
        # HOTFIX: this somehow introduced a unicode error on multiline strings with the character
        # \u0007 (bell) in them. Commenting out the below code is a temporary fix.

        # yaml.Constructor = ZenlyticConstructor
        # yaml.representer.add_representer(Str, ProjectReaderBase.repr_str)
        # yaml.representer.add_representer(ZenlyticPreservedScalarString, ProjectReaderBase.repr_str)
        # yaml.representer.add_representer(ZenlyticDoubleQuotedScalarString, ProjectReaderBase.repr_str)
        # yaml.representer.add_representer(ZenlyticSingleQuotedScalarString, ProjectReaderBase.repr_str)
        filtered_data = {k: v for k, v in data.items() if not k.startswith("_")}
        with open(path, "w") as f:
            yaml.dump(filtered_data, f)

    def load(self) -> None:
        raise NotImplementedError()
