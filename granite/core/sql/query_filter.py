import datetime
from copy import deepcopy
from enum import Enum
from typing import Dict

# from pypika import functions as fn
from pypika import Criterion, Field
from pypika.terms import LiteralValue

from granite.core.model.base import GraniteBase
from granite.core.sql.query_errors import ParseError


class GraniteFilterExpressionType(str, Enum):
    Unknown = "UNKNOWN"
    LessThan = "less_than"
    LessOrEqualThan = "less_or_equal_than"
    EqualTo = "equal_to"
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

    definition: {key, "expression", "value"}
    """

    def __init__(
        self, definition: Dict = {}, literal_filter: str = None, design=None, pivot_date: str = None
    ) -> None:
        definition = deepcopy(definition)

        # The design is used for filters in queries against specific designs
        #  to validate that all the tables and attributes (columns/aggregates)
        #  are properly defined in the design
        self.design = design
        self.literal_filter = literal_filter
        self.query_type = self.design.query_type

        self.validate(definition)

        self.expression_type = GraniteFilterExpressionType.parse(definition["expression"])

        # Some Query definitions may have a pivot_date set in the `today` param
        # We use that pivot_date to define relative date filter
        #  around that date and not NOW()
        self.pivot_date = pivot_date

        # Check if there are relative date filter definitions, parse them
        #  and generate the proper SQL clauses
        self.parse_relative_date_filters(definition)

        super().__init__(definition)

    def validate(self, definition: Dict) -> None:
        """
        Validate the Filter definition
        """
        key = definition.get("key", None)

        if key is None:
            raise ParseError(f"An attribute key was not provided for filter '{definition}'.")

        if definition["expression"] == "UNKNOWN":
            raise NotImplementedError(f"Unknown filter expression: {definition['expression']}.")

        if (
            definition.get("value", None) is None
            and definition["expression"] != "is_null"
            and definition["expression"] != "is_not_null"
        ):
            raise ParseError(f"Filter expression: {definition['expression']} needs a non-empty value.")

        if self.design:
            logger.info(key)
            # Will raise ParseError if not found
            _, self.attribute_name = key.split(".")
            # This handles the case that the attribute is a dimension group which we
            # currently always read as the raw group in the date field
            try:
                self.view_name = self.design.resolve_view_name(self.attribute_name)
                attribute_def = self.design.get_field(self.attribute_name, self.view_name)
            except ParseError:
                self.attribute_name += "_raw"
                self.view_name = self.design.resolve_view_name(self.attribute_name)
                attribute_def = self.design.get_field(self.attribute_name, self.view_name)

            logger.info("WWW")
            logger.info(type(definition["value"]))
            logger.info(attribute_def)
            logger.info(attribute_def.type)

            if self.query_type == "BIGQUERY" and isinstance(definition["value"], datetime.datetime):
                cast_func = "DATETIME" if attribute_def.datatype == "date" else "TIMESTAMP"
                definition["value"] = LiteralValue(f"{cast_func}('{definition['value']}')")

            if attribute_def.type == "yesno" and "False" in definition["value"]:
                definition["expression"] = "boolean_false"

            if attribute_def.type == "yesno" and "True" in definition["value"]:
                definition["expression"] = "boolean_true"

    def match(self, attribute: GraniteBase) -> bool:
        """
        Return True if this filter is defined for the given attribute
        """
        return self.attribute_name == attribute.alias() and self.view_name == attribute.view_name

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
