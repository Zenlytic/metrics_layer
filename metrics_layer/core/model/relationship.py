from typing import TYPE_CHECKING

import sqlglot

from metrics_layer.core.exceptions import QueryError

from .base import MetricsLayerBase, SQLReplacement
from .definitions import sql_flavor_to_sqlglot_format
from .field import Field
from .join import Join, ZenlyticJoinRelationship, ZenlyticJoinType

if TYPE_CHECKING:
    from metrics_layer.core.model.model import Model


class Relationship(MetricsLayerBase, SQLReplacement):
    """
    Represents a relationship between two views in a data model.

    This class stores metadata about join relationships that can be shown
    to language models or used for documentation purposes.
    """

    valid_properties = ["from_table", "join_table", "join_type", "relationship", "sql_on"]

    def __init__(self, definition: dict, model: "Model") -> None:
        self.model = model
        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        """Validate that the relationship has all required keys."""
        required_keys = ["from_table", "join_table", "sql_on"]

        for k in required_keys:
            if k not in definition:
                from_table = definition.get("from_table", "unknown")
                join_table = definition.get("join_table", "unknown")
                raise QueryError(
                    f"Relationship missing required key '{k}' in relationship "
                    f"between {from_table} and {join_table}"
                )

    @property
    def name(self):
        """Return a descriptive name for this relationship."""
        return f"relationship between {self.from_table} and {self.join_table}"

    @property
    def relationship(self):
        return self._definition.get("relationship", ZenlyticJoinRelationship.many_to_one)

    @property
    def join_type(self):
        return self._definition.get("join_type", ZenlyticJoinType.left_outer)

    def _error(self, element, error, extra: dict = {}):
        """Generate an error dictionary with context about this relationship."""
        line, column = self.line_col(element)
        return {
            **extra,
            "model_name": self.model.name,
            "message": error,
            "line": line,
            "column": column,
            "reference_type": "model",
            "reference_id": self.model.name,
        }

    def collect_errors(self):
        """Validate all properties of the relationship and collect any errors."""
        errors = []

        # Validate from_table
        if not isinstance(self.from_table, str):
            errors.append(
                self._error(
                    self.from_table,
                    f"The from_table property, {self.from_table} must be a string in {self.name} "
                    f"in the model {self.model.name}",
                )
            )
        else:
            # Check if the view exists
            try:
                self.model.project.get_view(self.from_table)
            except Exception:
                errors.append(
                    self._error(
                        self.from_table,
                        f"The from_table property, {self.from_table} does not reference a valid view "
                        f"in {self.name} in the model {self.model.name}",
                    )
                )

        # Validate join_table
        if not isinstance(self.join_table, str):
            errors.append(
                self._error(
                    self.join_table,
                    f"The join_table property, {self.join_table} must be a string in {self.name} "
                    f"in the model {self.model.name}",
                )
            )
        else:
            # Check if the view exists
            try:
                self.model.project.get_view(self.join_table)
            except Exception:
                errors.append(
                    self._error(
                        self.join_table,
                        f"The join_table property, {self.join_table} does not reference a valid view "
                        f"in {self.name} in the model {self.model.name}",
                    )
                )

        # Validate join_type (optional)
        if "join_type" in self._definition:
            if not isinstance(self.join_type, str):
                errors.append(
                    self._error(
                        self.join_type,
                        f"The join_type property, {self.join_type} must be a string in {self.name} "
                        f"in the model {self.model.name}",
                    )
                )
            elif self.join_type not in ZenlyticJoinType.options:
                errors.append(
                    self._error(
                        self.join_type,
                        (
                            f"The join_type property, {self.join_type} must be one of "
                            f"{ZenlyticJoinType.options} in {self.name} in the model {self.model.name}"
                        ),
                    )
                )

        # Validate relationship
        if "relationship" in self._definition:
            if not isinstance(self.relationship, str):
                errors.append(
                    self._error(
                        self.relationship,
                        f"The relationship property, {self.relationship} must be a string in {self.name} "
                        f"in the model {self.model.name}",
                    )
                )
            elif self.relationship not in ZenlyticJoinRelationship.options:
                errors.append(
                    self._error(
                        self.relationship,
                        (
                            f"The relationship property, {self.relationship} must be one of "
                            f"{ZenlyticJoinRelationship.options} in {self.name} in the model {self.model.name}"
                        ),
                    )
                )

        # Validate sql_on
        if not isinstance(self.sql_on, str):
            errors.append(
                self._error(
                    self.sql_on,
                    f"The sql_on property, {self.sql_on} must be a string in {self.name} "
                    f"in the model {self.model.name}",
                )
            )
        else:
            errors.extend(
                Join.validate_sql_on_references(self.sql_on, self.name, self.model.project, self._error)
            )

        # Check for invalid properties
        errors.extend(
            self.invalid_property_error(
                self._definition,
                self.valid_properties,
                "relationship",
                self.name,
                error_func=self._error,
            )
        )

        return errors

    @staticmethod
    def static_sql_validation(sql_on: str, query_type: str, model):
        errors = []
        errors.extend(
            Join.validate_sql_on_references(sql_on, f"relationship {sql_on}", model.project, lambda s, x: x)
        )

        sqlglot_sql_flavor = sql_flavor_to_sqlglot_format(query_type)
        replaced_sql = Join.get_replaced_sql_on(sql_on, query_type, model.project)
        try:
            sqlglot.parse_one(replaced_sql, read=sqlglot_sql_flavor)
        except Exception as e:
            errors.append(str(e))

        return errors
