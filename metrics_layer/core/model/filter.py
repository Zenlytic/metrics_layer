from datetime import datetime
from enum import Enum

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
            cleaned_value = value[2:]

        # Numeric parsing for equal to, less than, greater than
        elif value[0] in {"=", ">", "<", "-"}:
            expression = _symbol_to_filter_type_lookup[value[0]]
            cleaned_value = value[1:]

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
            parsed_value = filter_dict["value"]

            # Handle null conditiona
            if filter_dict["expression"] == MetricsLayerFilterExpressionType.IsNull:
                condition_value = f"is null"
            elif filter_dict["expression"] == MetricsLayerFilterExpressionType.IsNotNull:
                condition_value = f"is not null"

            # isin or isnotin for strings
            elif filter_dict["expression"] == MetricsLayerFilterExpressionType.IsIn:
                categories = ",".join([f"'{v}'" for v in parsed_value])
                condition_value = f"is in ({categories})"

            elif filter_dict["expression"] == MetricsLayerFilterExpressionType.IsNotIn:
                categories = ",".join([f"'{v}'" for v in parsed_value])
                condition_value = f"is not in ({categories})"

            # Handle boolean True and False
            elif parsed_value == True or parsed_value == False:  # noqa
                condition_value = f"is {str(parsed_value).upper()}"

            # Handle date after and before
            elif f["value"].split(" ")[0] == "after":
                condition_value = f">= {parsed_value}"

            elif f["value"].split(" ")[0] == "before":
                condition_value = f"<= {parsed_value}"

            # Not equal to condition for strings
            elif f["value"][0] == "-":
                condition_value = f"<> '{parsed_value}'"

            # Numeric parsing for less than or equal to, greater than or equal to, not equal to
            elif f["value"][:2] in {"<=", ">=", "<>", "!="}:
                condition_value = f"{f['value'][:2]} {parsed_value}"

            # Numeric parsing for equal to, less than, greater than
            elif f["value"][0] in {"=", ">", "<"}:
                condition_value = f"{f['value'][0]} {parsed_value}"

            else:
                condition_value = f"= '{parsed_value}'"

            field_reference = "${" + f["field"] + "}"
            condition = f"{field_reference} {condition_value}"
            conditions.append(condition)

        # Add the filter conditions AND'd together
        case_sql += " and ".join(conditions)
        # Add the result from the sql arg + imply NULL for anything not hitting the filter condition
        case_sql += f" then {sql} end"

        return case_sql
