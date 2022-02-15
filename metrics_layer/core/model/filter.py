from datetime import datetime
from enum import Enum

import pandas as pd
from pypika.terms import LiteralValue

from .base import MetricsLayerBase


class MetricsLayerFilterExpressionType(str, Enum):
    Unknown = "UNKNOWN"
    LessThan = "less_than"
    LessOrEqualThan = "less_or_equal_than"
    EqualTo = "equal_to"
    NotEqualTo = "not_equal_to"
    GreaterOrEqualThan = "greater_or_equal_than"
    GreaterThan = "greater_than"
    Like = "like"
    Contains = "contains"
    DoesNotContain = "does_not_contain"
    ContainsCaseInsensitive = "contains_case_insensitive"
    DoesNotContainCaseInsensitive = "does_not_contain_case_insensitive"
    StartsWith = "starts_with"
    EndsWith = "ends_with"
    DoesNotStartWith = "does_not_start_with"
    DoesNotEndWith = "does_not_end_with"
    StartsWithCaseInsensitive = "starts_with_case_insensitive"
    EndsWithCaseInsensitive = "ends_with_case_insensitive"
    DoesNotStartWithCaseInsensitive = "does_not_start_with_case_insensitive"
    DoesNotEndWithCaseInsensitive = "does_not_end_with_case_insensitive"
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


class Filter(MetricsLayerBase):
    def __init__(self, definition: dict = {}) -> None:
        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["field", "value"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Filter missing required key '{k}' The filter passed was {definition}")

    def filter_dict(self, json_safe: bool = False):
        filter_dict = self._filter_dict(self.field, self.value)
        enriched_filter = {**self._definition, **filter_dict}
        if json_safe:
            enriched_filter["expression"] = enriched_filter["expression"].value
        return enriched_filter

    @staticmethod
    def _filter_dict(field: str, value: str):
        # TODO more advanced parsing similar to
        # ref: https://docs.looker.com/reference/field-params/filters

        _date_values_lookup = {
            "yesterday",
            "last week",
            "last month",
            "last quarter",
            "last year",
            "week to date",
            "month to date",
            "quarter to date",
            "year to date",
            "last week to date",
            "last month to date",
            "last quarter to date",
            "last year to date",
        }
        _symbol_to_filter_type_lookup = {
            "<=": MetricsLayerFilterExpressionType.LessOrEqualThan,
            ">=": MetricsLayerFilterExpressionType.GreaterOrEqualThan,
            "<>": MetricsLayerFilterExpressionType.NotEqualTo,
            "!=": MetricsLayerFilterExpressionType.NotEqualTo,
            "-": MetricsLayerFilterExpressionType.NotEqualTo,
            "=": MetricsLayerFilterExpressionType.EqualTo,
            ">": MetricsLayerFilterExpressionType.GreaterThan,
            "<": MetricsLayerFilterExpressionType.LessThan,
        }

        # Handle null conditional
        if value == "NULL":
            expression = MetricsLayerFilterExpressionType.IsNull
            cleaned_value = None

        elif value == "-NULL":
            expression = MetricsLayerFilterExpressionType.IsNotNull
            cleaned_value = None

        # Handle boolean True and False
        elif value == True or value == False:  # noqa
            expression = MetricsLayerFilterExpressionType.EqualTo
            cleaned_value = value

        # Handle date conditions
        elif value in _date_values_lookup:
            raise
            start, end = Filter._parse_date_string(value.split(" ")[-1])
            if value.split(" ")[0] == "after":
                expression = MetricsLayerFilterExpressionType.GreaterOrEqualThan
            else:
                expression = MetricsLayerFilterExpressionType.LessOrEqualThan

        # Handle date after and before
        elif value.split(" ")[0] in {"after", "before"}:
            cleaned_value = Filter._parse_date_string(value.split(" ")[-1])
            if value.split(" ")[0] == "after":
                expression = MetricsLayerFilterExpressionType.GreaterOrEqualThan
            else:
                expression = MetricsLayerFilterExpressionType.LessOrEqualThan

        # isin for strings
        elif len(value.split(", ")) > 1:
            if all(category[0] == "-" for category in value.split(", ")):
                expression = MetricsLayerFilterExpressionType.IsNotIn
                cleaned_value = [f"{category[1:].strip()}" for category in value.split(", ")]

            elif any(category[0] == "-" for category in value.split(", ")):
                raise ValueError("Invalid filter some elements are negated with '-' and some are not")

            else:
                expression = MetricsLayerFilterExpressionType.IsIn
                cleaned_value = [f"{category.strip()}" for category in value.split(", ")]

        # Numeric parsing for less than or equal to, greater than or equal to, not equal to
        elif value[:2] in {"<=", ">=", "<>", "!="}:
            expression = _symbol_to_filter_type_lookup[value[:2]]
            cleaned_value = pd.to_numeric(value[2:])

        # Numeric parsing for equal to, less than, greater than
        elif value[0] in {"=", ">", "<"}:
            expression = _symbol_to_filter_type_lookup[value[0]]
            cleaned_value = pd.to_numeric(value[1:])

        # String parsing for NOT equal to
        elif value[0] == "-":
            if value[1] == "%" and value[-1] == "%":
                expression = MetricsLayerFilterExpressionType.DoesNotContainCaseInsensitive
                cleaned_value = value[2:-1]
            elif value[1] == "%":
                expression = MetricsLayerFilterExpressionType.DoesNotEndWithCaseInsensitive
                cleaned_value = value[2:]
            elif value[-1] == "%":
                expression = MetricsLayerFilterExpressionType.DoesNotStartWithCaseInsensitive
                cleaned_value = value[1:-1]
            else:
                expression = _symbol_to_filter_type_lookup[value[0]]
                cleaned_value = value[1:]

        else:
            if value[0] == "%" and value[-1] == "%":
                expression = MetricsLayerFilterExpressionType.ContainsCaseInsensitive
                cleaned_value = value[1:-1]
            elif value[0] == "%":
                expression = MetricsLayerFilterExpressionType.EndsWithCaseInsensitive
                cleaned_value = value[1:]
            elif value[-1] == "%":
                expression = MetricsLayerFilterExpressionType.StartsWithCaseInsensitive
                cleaned_value = value[:-1]
            else:
                expression = MetricsLayerFilterExpressionType.EqualTo
                cleaned_value = value

        return {"field": field, "expression": expression, "value": cleaned_value}

    @staticmethod
    def _parse_date_string(date_string: str):
        parsed_date = datetime.strptime(date_string, "%Y-%m-%d")
        return parsed_date.strftime("%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def translate_looker_filters_to_sql(sql: str, filters: list):
        case_sql = "case when "
        conditions = []
        for f in filters:
            filter_dict = Filter._filter_dict(f["field"], f["value"])

            field_reference = "${" + f["field"] + "}"
            condition_value = Filter.sql_query(
                field_reference, filter_dict["expression"], filter_dict["value"]
            )
            condition = f"{condition_value}"
            conditions.append(condition)

        # Add the filter conditions AND'd together
        case_sql += " and ".join(conditions)
        # Add the result from the sql arg + imply NULL for anything not hitting the filter condition
        case_sql += f" then {sql} end"

        return case_sql

    @staticmethod
    def sql_query(sql_to_compare: str, expression_type: str, value):
        field = LiteralValue(sql_to_compare)
        criterion_strategies = {
            MetricsLayerFilterExpressionType.LessThan: lambda f: f < value,
            MetricsLayerFilterExpressionType.LessOrEqualThan: lambda f: f <= value,
            MetricsLayerFilterExpressionType.EqualTo: lambda f: f == value,
            MetricsLayerFilterExpressionType.NotEqualTo: lambda f: f != value,
            MetricsLayerFilterExpressionType.GreaterOrEqualThan: lambda f: f >= value,
            MetricsLayerFilterExpressionType.GreaterThan: lambda f: f > value,
            MetricsLayerFilterExpressionType.Like: lambda f: f.like(value),
            MetricsLayerFilterExpressionType.Contains: lambda f: f.like(f"%{value}%"),
            MetricsLayerFilterExpressionType.DoesNotContain: lambda f: f.not_like(f"%{value}%"),
            MetricsLayerFilterExpressionType.ContainsCaseInsensitive: lambda f: f.ilike(f"%{value}%"),
            MetricsLayerFilterExpressionType.DoesNotContainCaseInsensitive: lambda f: f.not_ilike(
                f"%{value}%"
            ),
            MetricsLayerFilterExpressionType.StartsWith: lambda f: f.like(f"{value}%"),
            MetricsLayerFilterExpressionType.EndsWith: lambda f: f.like(f"%{value}"),
            MetricsLayerFilterExpressionType.DoesNotStartWith: lambda f: f.not_like(f"{value}%"),
            MetricsLayerFilterExpressionType.DoesNotEndWith: lambda f: f.not_like(f"%{value}"),
            MetricsLayerFilterExpressionType.StartsWithCaseInsensitive: lambda f: f.ilike(f"{value}%"),
            MetricsLayerFilterExpressionType.EndsWithCaseInsensitive: lambda f: f.ilike(f"%{value}"),
            MetricsLayerFilterExpressionType.DoesNotStartWithCaseInsensitive: lambda f: f.not_ilike(
                f"{value}%"
            ),
            MetricsLayerFilterExpressionType.DoesNotEndWithCaseInsensitive: lambda f: f.not_ilike(
                f"%{value}"
            ),
            MetricsLayerFilterExpressionType.IsNull: lambda f: f.isnull(),
            MetricsLayerFilterExpressionType.IsNotNull: lambda f: f.notnull(),
            MetricsLayerFilterExpressionType.IsIn: lambda f: f.isin(value),
            MetricsLayerFilterExpressionType.IsNotIn: lambda f: f.isin(value).negate(),
            MetricsLayerFilterExpressionType.BooleanTrue: lambda f: f,
            MetricsLayerFilterExpressionType.BooleanFalse: lambda f: f.negate(),
        }

        try:
            return criterion_strategies[expression_type](field)
        except KeyError:
            raise NotImplementedError(f"Unknown filter expression_type: {expression_type}.")
