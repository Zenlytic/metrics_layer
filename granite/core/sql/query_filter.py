import datetime
from copy import deepcopy
from enum import Enum
from typing import Dict

import sqlparse
from pypika import Criterion, Field
from pypika.terms import LiteralValue
from sqlparse.tokens import Error, Name

from granite.core.model.base import GraniteBase
from granite.core.model.field import Field as GraniteField
from granite.core.sql.pypika_types import LiteralValueCriterion
from granite.core.sql.query_errors import ParseError


class GraniteFilterExpressionType(str, Enum):
    Unknown = "UNKNOWN"
    LessThan = "less_than"
    LessOrEqualThan = "less_or_equal_than"
    EqualTo = "equal_to"
    NotEqualTo = "not_equal_to"
    GreaterOrEqualThan = "greater_or_equal_than"
    GreaterThan = "greater_than"
    Like = "like"
    IsNull = "is_null"
    IsNotNull = "is_not_null"
    IsIn = "isin"
    IsNotIn = "isnotin"
    BooleanTrue = "boolean_true"
    BooleanFalse = "boolean_false"

    def __hash__(self):
        return hash(self.value)

    def __eq__(self, other):
        return self.value.lower() == other.value.lower()

    @classmethod
    def parse(cls, value: str):
        try:
            return next(e for e in cls if e.value.lower() == value)
        except StopIteration:
            return cls.Unknown


class GraniteFilter(GraniteBase):
    """
    An internal representation of a Filter (WHERE or HAVING clause)
    defined in a GraniteQuery.

    definition: {"field", "expression", "value"}
    """

    def __init__(self, definition: Dict = {}, design=None, filter_type: str = None) -> None:
        definition = deepcopy(definition)

        # The design is used for filters in queries against specific designs
        #  to validate that all the tables and attributes (columns/aggregates)
        #  are properly defined in the design
        self.design = design
        self.is_literal_filter = "literal" in definition
        self.query_type = self.design.query_type
        self.filter_type = filter_type

        self.validate(definition)

        if not self.is_literal_filter:
            self.expression_type = GraniteFilterExpressionType.parse(definition["expression"])

        super().__init__(definition)

    def validate(self, definition: Dict) -> None:
        """
        Validate the Filter definition
        """
        key = definition.get("field", None)
        filter_literal = definition.get("literal", None)

        if key is None and filter_literal is None:
            raise ParseError(f"An attribute key or literal was not provided for filter '{definition}'.")

        if key is None and filter_literal:
            return

        if definition["expression"] == "UNKNOWN":
            raise NotImplementedError(f"Unknown filter expression: {definition['expression']}.")

        if (
            definition.get("value", None) is None
            and definition["expression"] != "is_null"
            and definition["expression"] != "is_not_null"
        ):
            raise ParseError(f"Filter expression: {definition['expression']} needs a non-empty value.")

        if self.design:
            # Will raise ParseError if not found
            try:
                self.field = self.design.get_field(key)
            except ParseError:
                raise ParseError(
                    f"We could not find field {self.field_name} in explore {self.design.explore.name}"
                )

            if self.design.query_type == "BIGQUERY" and isinstance(definition["value"], datetime.datetime):
                cast_func = "DATETIME" if self.field.datatype == "date" else "TIMESTAMP"
                definition["value"] = LiteralValue(f"{cast_func}('{definition['value']}')")

            if self.field.type == "yesno" and "False" in definition["value"]:
                definition["expression"] = "boolean_false"

            if self.field.type == "yesno" and "True" in definition["value"]:
                definition["expression"] = "boolean_true"

    def sql_query(self):
        if self.is_literal_filter:
            return LiteralValueCriterion(self.replace_fields_literal_filter())
        return self.criterion(LiteralValue(self.field.sql_query()))

    def replace_fields_literal_filter(self):
        generator = sqlparse.parse(self.literal)[0].flatten()
        tokens = []
        for token in generator:
            if token.ttype == Name:
                field = self.design.get_field(str(token))
                tokens.append("${" + field.view.name + "." + field.name + "}")
            elif token.ttype != Error:
                tokens.append(str(token))

        if self.filter_type == "where":
            extra_args = {"field_type": None}
        else:
            extra_args = {"field_type": "measure", "type": "number"}
        view = self.design.get_view(self.design.base_view_name)
        field = GraniteField({"sql": "".join(tokens), "name": None, **extra_args}, view=view)
        return field.sql_query()

    def criterion(self, field: Field) -> Criterion:
        """
        Generate the Pypika Criterion for this filter

        field: Pypika Field as we don't know the base table this filter is
                evaluated against (is it the base table or an intermediary table?)

        We have to use the following cases as PyPika does not allow an str
         representation of the clause on its where() and having() functions
        """
        criterion_strategies = {
            GraniteFilterExpressionType.LessThan: lambda f: f < self.value,
            GraniteFilterExpressionType.LessOrEqualThan: lambda f: f <= self.value,
            GraniteFilterExpressionType.EqualTo: lambda f: f == self.value,
            GraniteFilterExpressionType.NotEqualTo: lambda f: f != self.value,
            GraniteFilterExpressionType.GreaterOrEqualThan: lambda f: f >= self.value,
            GraniteFilterExpressionType.GreaterThan: lambda f: f > self.value,
            GraniteFilterExpressionType.Like: lambda f: f.like(self.value),
            GraniteFilterExpressionType.IsNull: lambda f: f.isnull(),
            GraniteFilterExpressionType.IsNotNull: lambda f: f.notnull(),
            GraniteFilterExpressionType.IsIn: lambda f: f.isin(self.value),
            GraniteFilterExpressionType.IsNotIn: lambda f: f.isin(self.value).negate(),
            GraniteFilterExpressionType.BooleanTrue: lambda f: f,
            GraniteFilterExpressionType.BooleanFalse: lambda f: f.negate(),
        }

        try:
            return criterion_strategies[self.expression_type](field)
        except KeyError:
            raise NotImplementedError(f"Unknown filter expression_type: {self.expression_type}.")
