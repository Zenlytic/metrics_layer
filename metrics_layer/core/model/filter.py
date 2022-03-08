import re
from datetime import datetime
from enum import Enum

import pandas as pd
import pendulum
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
    Matches = "matches"

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


class FilterInterval(str, Enum):
    unknown = "UNKNOWN"
    day = "day"
    week = "week"
    month = "month"
    quarter = "quarter"
    year = "year"

    def __hash__(self):
        return hash(self.value)

    def __eq__(self, other):
        return self.value.lower() == other.value.lower()

    @staticmethod
    def plural(value: str):
        return value.lower() if value[-1] == "s" else f"{value.lower()}s"

    @staticmethod
    def singular(value: str):
        return value.lower()[:-1] if value[-1] == "s" else value.lower()

    @classmethod
    def all(cls):
        return [i for e in cls for i in [e.value.lower(), f"{e.value.lower()}s"]]

    @classmethod
    def parse(cls, value: str):
        try:
            return next(e for e in cls if value in {e.value.lower(), f"{e.value.lower()}s"})
        except StopIteration:
            return cls.unknown


class Filter(MetricsLayerBase):
    def __init__(self, definition: dict = {}) -> None:
        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["field", "value"]
        for k in required_keys:
            if k not in definition:
                raise ValueError(f"Filter missing required key '{k}' The filter passed was {definition}")

    def filter_dict(self, json_safe: bool = False) -> list:
        filter_dict = self._filter_dict(self.field, self.value)
        if isinstance(filter_dict, dict):
            filter_list = [filter_dict]
        else:
            filter_list = filter_dict
        return [self._clean_filter_dict({**self._definition, **f}, json_safe) for f in filter_list]

    @staticmethod
    def _clean_filter_dict(filter_dict: dict, json_safe: bool):
        if json_safe:
            filter_dict["expression"] = filter_dict["expression"].value
        return filter_dict

    @staticmethod
    def _end_date(lag: int, date_part: str):
        plural_date_part = FilterInterval.plural(date_part)
        singular_date_part = FilterInterval.singular(date_part)
        now = Filter._utc_today()

        if singular_date_part == "quarter":
            date = now.subtract(months=3 * lag).last_of("quarter")
        else:
            date = now.subtract(**{plural_date_part: lag}).end_of(singular_date_part)
        return Filter._date_to_string(date)

    @staticmethod
    def _start_date(lag: int, date_part: str, return_date=False):
        plural_date_part = FilterInterval.plural(date_part)
        singular_date_part = FilterInterval.singular(date_part)
        now = Filter._utc_today()

        if singular_date_part == "quarter":
            date = now.subtract(months=3 * lag).first_of("quarter")
        else:
            date = now.subtract(**{plural_date_part: lag}).start_of(singular_date_part)
        if return_date:
            return date
        return Filter._date_to_string(date)

    def _add_to_end_date(date, lag: int, date_part: str):
        plural_date_part = FilterInterval.plural(date_part)
        singular_date_part = FilterInterval.singular(date_part)
        if singular_date_part == "quarter":
            date = date.add(months=3 * lag).last_of("quarter")
        else:
            date = date.add(**{plural_date_part: lag}).end_of(singular_date_part)
        return date

    @staticmethod
    def parse_date_condition(date_condition: str):
        if date_condition == "today":
            expression = MetricsLayerFilterExpressionType.GreaterOrEqualThan
            cleaned_value = Filter._start_date(lag=0, date_part=FilterInterval.day)
            return [(expression, cleaned_value)]

        if date_condition == "yesterday":
            start_expression = MetricsLayerFilterExpressionType.GreaterOrEqualThan
            start_value = Filter._start_date(lag=1, date_part=FilterInterval.day)

            end_expression = MetricsLayerFilterExpressionType.LessOrEqualThan
            end_value = Filter._end_date(lag=1, date_part=FilterInterval.day)
            return [(start_expression, start_value), (end_expression, end_value)]

        # Match regex patterns to the various date filters and when patterns
        # match assume it's a date not a string comparison
        interval_condition = "|".join(FilterInterval.all())
        interval_ago_for_result = Filter._parse_n_interval_ago_for(date_condition, interval_condition)
        if interval_ago_for_result:
            return interval_ago_for_result

        interval_modifier_result = Filter._parse_n_interval_modifier(date_condition, interval_condition)
        if interval_modifier_result:
            return interval_modifier_result

        n_interval_result = Filter._parse_n_interval(date_condition, interval_condition)
        if n_interval_result:
            return n_interval_result

    @staticmethod
    def _parse_n_interval(date_condition: str, interval_condition: str):
        regex = rf"(\d+|this|last)\s+({interval_condition})"  # noqa
        result = re.search(regex, str(date_condition))
        if not result:
            return

        n = result.group(1)
        date_part = result.group(2)

        if n == "this":
            lag = 0
        elif n == "last":
            lag = 1
        else:
            lag = int(n) - 1

        start_expression = MetricsLayerFilterExpressionType.GreaterOrEqualThan
        start_value = Filter._start_date(lag=lag, date_part=date_part)
        result = [(start_expression, start_value)]

        if n == "last":
            end_expression = MetricsLayerFilterExpressionType.LessOrEqualThan
            end_value = Filter._end_date(lag=lag, date_part=date_part)
            result.append((end_expression, end_value))

        return result

    @staticmethod
    def _parse_n_interval_modifier(date_condition: str, interval_condition: str):
        regex = rf"(\d+\s+|this\s+|last\s+|)({interval_condition})\s+(ago\s+to\s+date|ago|to\s+date)"  # noqa
        result = re.search(regex, str(date_condition))
        if not result:
            return

        n = result.group(1).strip()
        date_part = result.group(2)
        modifier = result.group(3)

        if n in {"this", ""}:
            lag = 0
        elif n == "last":
            lag = 1
        elif modifier == "to date" and n != "last":
            lag = int(n) - 1
        else:
            lag = int(n)

        start_expression = MetricsLayerFilterExpressionType.GreaterOrEqualThan
        start_date = Filter._start_date(lag=lag, date_part=date_part, return_date=True)
        result = [(start_expression, Filter._date_to_string(start_date))]

        if modifier == "ago to date" or modifier == "to date":
            end_expression = MetricsLayerFilterExpressionType.LessOrEqualThan

            # We need to figure out how many days are between now and the start of the period
            start_of_period = Filter._start_date(lag=0, date_part=date_part, return_date=True)
            time_change = Filter._utc_today() - start_of_period
            end_date = Filter._add_to_end_date(start_date, lag=time_change.days - 1, date_part="day")
            result.append((end_expression, Filter._date_to_string(end_date)))

        elif modifier == "ago":
            end_expression = MetricsLayerFilterExpressionType.LessOrEqualThan
            end_value = Filter._end_date(lag=lag, date_part=date_part)
            result.append((end_expression, end_value))

        return result

    @staticmethod
    def _parse_n_interval_ago_for(date_condition: str, interval_condition: str):
        regex = rf"(\d+)\s+({interval_condition})\s+ago\s+for\s+(\d+)\s+({interval_condition})"  # noqa
        result = re.search(regex, str(date_condition))
        if not result:
            return

        first_n = int(result.group(1))
        first_date_part = result.group(2)
        second_n = int(result.group(3)) - 1
        second_date_part = result.group(4)

        start_expression = MetricsLayerFilterExpressionType.GreaterOrEqualThan
        start_date = Filter._start_date(lag=first_n, date_part=first_date_part, return_date=True)
        result = [(start_expression, Filter._date_to_string(start_date))]

        end_expression = MetricsLayerFilterExpressionType.LessOrEqualThan
        end_date = Filter._add_to_end_date(start_date, lag=second_n, date_part=second_date_part)
        result.append((end_expression, Filter._date_to_string(end_date)))

        return result

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
        date_condition = Filter.parse_date_condition(value)

        # Handle null conditional
        if value == "NULL":
            expression = MetricsLayerFilterExpressionType.IsNull
            cleaned_value = None

        elif value == "-NULL":
            expression = MetricsLayerFilterExpressionType.IsNotNull
            cleaned_value = None

        # Handle boolean True and False
        elif value in {True, False, "TRUE", "FALSE"}:  # noqa
            expression = MetricsLayerFilterExpressionType.EqualTo
            if value in {True, "TRUE"}:
                cleaned_value = True
            elif value in {False, "FALSE"}:
                cleaned_value = False

        # Handle date conditions
        elif date_condition:
            multiple_filter_dicts = []
            for expression, cleaned_value in date_condition:
                filter_dict = {"field": field, "expression": expression, "value": cleaned_value}
                multiple_filter_dicts.append(filter_dict)
            if len(multiple_filter_dicts) == 1:
                return multiple_filter_dicts[0]
            return multiple_filter_dicts

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
        return Filter._date_to_string(parsed_date)

    @staticmethod
    def _utc_today():
        return pendulum.now("UTC")

    @staticmethod
    def _date_to_string(date_obj):
        return date_obj.strftime("%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def translate_looker_filters_to_sql(sql: str, filters: list):
        case_sql = "case when "
        conditions = []
        for f in filters:
            filter_dict = Filter._filter_dict(f["field"], f["value"])
            if isinstance(filter_dict, dict):
                filter_list = [filter_dict]
            else:
                filter_list = filter_dict

            for filter_obj in filter_list:
                field_reference = "${" + f["field"] + "}"
                condition_value = Filter.sql_query(
                    field_reference, filter_obj["expression"], filter_obj["value"]
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
