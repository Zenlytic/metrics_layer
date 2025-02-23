import re
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Callable

import pandas as pd
import pendulum
from pypika import Criterion
from pypika.functions import Lower
from pypika.terms import LiteralValue

from metrics_layer.core.exceptions import QueryError

from .base import MetricsLayerBase
from .week_start_day_types import WeekStartDayTypes

if TYPE_CHECKING:
    from metrics_layer.core.model.view import View


class LiteralValueCriterion(Criterion):
    def __init__(self, sql_query: str, alias: str = None) -> None:
        """A wrapper for a literal value criterion which is a string of valid sql"""
        super().__init__(alias)
        self.sql_query = sql_query

    def get_sql(self, **kwargs):
        return self.sql_query


class MetricsLayerFilterGroupLogicalOperatorType:
    and_ = "AND"
    or_ = "OR"
    options = [and_, or_]


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
    IsInQuery = "is_in_query"
    IsNotInQuery = "is_not_in_query"
    BooleanTrue = "boolean_true"
    BooleanFalse = "boolean_false"
    IsTrue = "is_true"
    IsFalse = "is_false"
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
    week_start_day_default = pendulum.MONDAY
    week_end_day_default = pendulum.SUNDAY
    week_start_day_lookup = {
        WeekStartDayTypes.monday: pendulum.MONDAY,
        WeekStartDayTypes.tuesday: pendulum.TUESDAY,
        WeekStartDayTypes.wednesday: pendulum.WEDNESDAY,
        WeekStartDayTypes.thursday: pendulum.THURSDAY,
        WeekStartDayTypes.friday: pendulum.FRIDAY,
        WeekStartDayTypes.saturday: pendulum.SATURDAY,
        WeekStartDayTypes.sunday: pendulum.SUNDAY,
    }

    def __init__(self, definition: dict = {}) -> None:
        self.validate(definition)
        super().__init__(definition)

    def validate(self, definition: dict):
        required_keys = ["field", "value"]
        for k in required_keys:
            if k not in definition:
                raise QueryError(f"Filter missing required key '{k}' The filter passed was {definition}")

    def filter_dict(self, json_safe: bool = False) -> list:
        filter_dict = self._filter_dict(self.field, self.value, self.week_start_day, self.timezone)
        if isinstance(filter_dict, dict):
            filter_list = [filter_dict]
        else:
            filter_list = filter_dict
        return [self._clean_filter_dict({**self._definition, **f}, json_safe) for f in filter_list]

    @staticmethod
    def _clean_filter_dict(filter_dict: dict, json_safe: bool):
        if json_safe and "expression" in filter_dict:
            filter_dict["expression"] = filter_dict["expression"].value
        return filter_dict

    @staticmethod
    def _set_week_start_day(week_start_day: str):
        week_start_day = Filter.week_start_day_lookup.get(week_start_day, Filter.week_start_day_default)
        pendulum.week_starts_at(week_start_day)
        week_end_day = week_start_day - 1 if week_start_day != 0 else 6
        pendulum.week_ends_at(week_end_day)

    @staticmethod
    def _reset_week_start_day():
        pendulum.week_starts_at(Filter.week_start_day_default)
        pendulum.week_ends_at(Filter.week_end_day_default)

    @staticmethod
    def _end_date(lag: int, date_part: str, tz: str):
        plural_date_part = FilterInterval.plural(date_part)
        singular_date_part = FilterInterval.singular(date_part)
        now = Filter._today(tz=tz)

        if singular_date_part == "quarter":
            date = now.subtract(months=3 * lag).last_of("quarter")
        else:
            date = now.subtract(**{plural_date_part: lag}).end_of(singular_date_part)
        return Filter._date_to_string(date)

    @staticmethod
    def _start_date(lag: int, date_part: str, tz: str, return_date=False):
        plural_date_part = FilterInterval.plural(date_part)
        singular_date_part = FilterInterval.singular(date_part)
        now = Filter._today(tz=tz)

        if singular_date_part == "quarter":
            date = now.subtract(months=3 * lag).first_of("quarter")
        else:
            date = now.subtract(**{plural_date_part: lag}).start_of(singular_date_part)
        if return_date:
            return date
        return Filter._date_to_string(date)

    @staticmethod
    def _add_to_end_date(date, lag: int, date_part: str):
        plural_date_part = FilterInterval.plural(date_part)
        singular_date_part = FilterInterval.singular(date_part)
        if singular_date_part == "quarter":
            date = date.add(months=3 * lag).last_of("quarter")
        else:
            date = date.add(**{plural_date_part: lag}).end_of(singular_date_part)
        return date

    @staticmethod
    def parse_date_condition(date_condition: str, tz: str) -> list:
        if tz is None:
            tz = "UTC"

        if " until " in str(date_condition):
            first, second = date_condition.split(" until ")

            start_expression = MetricsLayerFilterExpressionType.GreaterOrEqualThan
            parsed_first = Filter.parse_date_condition(first, tz)
            if parsed_first is None:
                start_value = Filter._parse_date_string(first)
                first_filter = (start_expression, start_value)
            else:
                first_filter = parsed_first[0]

            end_expression = MetricsLayerFilterExpressionType.LessOrEqualThan
            parsed_second = Filter.parse_date_condition(second, tz)
            if parsed_second is None:
                end_value = Filter._parse_date_string(second)
                second_filter = (end_expression, end_value)
            else:
                second_filter = parsed_second[-1]

            return [first_filter, second_filter]

        if date_condition == "today":
            expression = MetricsLayerFilterExpressionType.GreaterOrEqualThan
            cleaned_value = Filter._start_date(lag=0, date_part=FilterInterval.day, tz=tz)
            end_expression = MetricsLayerFilterExpressionType.LessOrEqualThan
            end_value = Filter._end_date(lag=0, date_part=FilterInterval.day, tz=tz)
            return [(expression, cleaned_value), (end_expression, end_value)]

        if date_condition == "yesterday":
            start_expression = MetricsLayerFilterExpressionType.GreaterOrEqualThan
            start_value = Filter._start_date(lag=1, date_part=FilterInterval.day, tz=tz)

            end_expression = MetricsLayerFilterExpressionType.LessOrEqualThan
            end_value = Filter._end_date(lag=1, date_part=FilterInterval.day, tz=tz)
            return [(start_expression, start_value), (end_expression, end_value)]

        # Match regex patterns to the various date filters and when patterns
        # match assume it's a date not a string comparison
        interval_condition = "|".join(FilterInterval.all())
        interval_ago_for_result = Filter._parse_n_interval_ago_for(date_condition, interval_condition, tz=tz)
        if interval_ago_for_result:
            return interval_ago_for_result

        interval_modifier_result = Filter._parse_n_interval_modifier(
            date_condition, interval_condition, tz=tz
        )
        if interval_modifier_result:
            return interval_modifier_result

        n_interval_result = Filter._parse_n_interval(date_condition, interval_condition, tz=tz)
        if n_interval_result:
            return n_interval_result

    @staticmethod
    def _parse_n_interval(date_condition: str, interval_condition: str, tz: str):
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
        start_value = Filter._start_date(lag=lag, date_part=date_part, tz=tz)
        result = [(start_expression, start_value)]

        end_lag = lag if n == "last" else 0
        end_expression = MetricsLayerFilterExpressionType.LessOrEqualThan
        end_value = Filter._end_date(lag=end_lag, date_part=date_part, tz=tz)
        result.append((end_expression, end_value))

        return result

    @staticmethod
    def _parse_n_interval_modifier(date_condition: str, interval_condition: str, tz: str):
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
        start_date = Filter._start_date(lag=lag, date_part=date_part, tz=tz, return_date=True)
        result = [(start_expression, Filter._date_to_string(start_date))]

        if modifier == "ago to date" or modifier == "to date":
            end_expression = MetricsLayerFilterExpressionType.LessOrEqualThan

            # We need to figure out how many days are between now and the start of the period
            start_of_period = Filter._start_date(lag=0, date_part=date_part, tz=tz, return_date=True)
            time_change = Filter._today(tz=tz) - start_of_period
            lag = time_change.days - 1 if time_change.days > 0 else 0
            end_date = Filter._add_to_end_date(start_date, lag=lag, date_part="day")
            result.append((end_expression, Filter._date_to_string(end_date)))

        elif modifier == "ago":
            end_expression = MetricsLayerFilterExpressionType.LessOrEqualThan
            end_value = Filter._end_date(lag=lag, date_part=date_part, tz=tz)
            result.append((end_expression, end_value))

        return result

    @staticmethod
    def _parse_n_interval_ago_for(date_condition: str, interval_condition: str, tz: str):
        regex = rf"(\d+)\s+({interval_condition})\s+ago\s+for\s+(\d+)\s+({interval_condition})"  # noqa
        result = re.search(regex, str(date_condition))
        if not result:
            return

        first_n = int(result.group(1))
        first_date_part = result.group(2)
        second_n = int(result.group(3)) - 1
        second_date_part = result.group(4)

        start_expression = MetricsLayerFilterExpressionType.GreaterOrEqualThan
        start_date = Filter._start_date(lag=first_n, date_part=first_date_part, tz=tz, return_date=True)
        result = [(start_expression, Filter._date_to_string(start_date))]

        end_expression = MetricsLayerFilterExpressionType.LessOrEqualThan
        end_date = Filter._add_to_end_date(start_date, lag=second_n, date_part=second_date_part)
        result.append((end_expression, Filter._date_to_string(end_date)))

        return result

    @staticmethod
    def _filter_dict(field: str, value: str, week_start_day: str = None, tz: str = None):
        # TODO more advanced parsing similar to
        # ref: https://docs.looker.com/reference/field-params/filters

        # If the value is an empty dict, we should return None because the filters will be skipped
        if str(value) == "":
            return {}

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
        Filter._set_week_start_day(week_start_day)
        date_condition = Filter.parse_date_condition(value, tz=tz)
        Filter._reset_week_start_day()

        first_word = str(value).split(" ")[0]
        first_two_words = " ".join(str(value).split(" ")[:2])

        split_expression = ","
        # Handle field to field comparison
        if isinstance(value, LiteralValue):
            expression = MetricsLayerFilterExpressionType.EqualTo
            cleaned_value = value

        # Handle null conditional
        elif value == "NULL" or value is None:
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
        elif first_word in {"after", "before", "on"} or first_two_words in {"not on"}:
            cleaned_value = Filter._parse_date_string(value.split(" ")[-1])
            if first_word == "after":
                expression = MetricsLayerFilterExpressionType.GreaterOrEqualThan
            elif first_word == "before":
                expression = MetricsLayerFilterExpressionType.LessOrEqualThan
            elif first_word == "on":
                expression = MetricsLayerFilterExpressionType.EqualTo
            else:
                expression = MetricsLayerFilterExpressionType.NotEqualTo

        # isin for strings
        elif len(value.split(split_expression)) > 1:
            if all(category.strip()[0] == "-" for category in value.split(split_expression)):
                expression = MetricsLayerFilterExpressionType.IsNotIn
                cleaned_value = [
                    f"{category.strip()[1:].strip()}" for category in value.split(split_expression)
                ]

            elif any(category.strip()[0] == "-" for category in value.split(split_expression)):
                raise QueryError("Invalid filter some elements are negated with '-' and some are not")

            else:
                expression = MetricsLayerFilterExpressionType.IsIn
                cleaned_value = [f"{category.strip()}" for category in value.split(split_expression)]

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
    def _today(tz):
        timezone = pendulum.timezone(tz)
        return pendulum.now(timezone)

    @staticmethod
    def _date_to_string(date_obj):
        return date_obj.strftime("%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def translate_looker_filters_to_sql(
        sql: str,
        filters: list,
        view: "View",
        else_0: bool = False,
        sql_replacement_func: Callable = lambda x: x,
    ):
        case_sql = "case when "
        conditions = []
        for f in filters:
            # All filters must have both of these keys, otherwise they are invalid
            if not all(k in f for k in ["field", "value"]):
                continue

            if "." not in f["field"]:
                field_id = f'{view.name}.{f["field"]}'
            else:
                field_id = f["field"]
            try:
                field = view.project.get_field(field_id)
                field_datatype = field.type
            except Exception:
                field_datatype = "unknown"
            value = sql_replacement_func(f["value"])
            filter_dict = Filter._filter_dict(f["field"], value, f.get("week_start_day"), f.get("timezone"))
            if isinstance(filter_dict, dict):
                filter_list = [filter_dict]
            else:
                filter_list = filter_dict

            for filter_obj in filter_list:
                if filter_obj != {}:
                    field_reference = "${" + f["field"] + "}"
                    condition_value = Filter.sql_query(
                        field_reference, filter_obj["expression"], filter_obj["value"], field_datatype
                    )
                    condition = f"{condition_value}"
                    conditions.append(condition)

        # Add the filter conditions AND'd together
        case_sql += " and ".join(conditions)
        # Add the result from the sql arg + imply NULL for anything not hitting the filter condition
        if else_0:
            case_sql += f" then {sql} else 0 end"
        else:
            case_sql += f" then {sql} end"

        return case_sql

    @staticmethod
    def sql_query(sql_to_compare: str, expression_type: str, value, field_datatype: str):
        field = LiteralValue(sql_to_compare)
        if (
            expression_type
            in {MetricsLayerFilterExpressionType.IsIn, MetricsLayerFilterExpressionType.IsNotIn}
            and field_datatype == "number"
        ):
            value = [pd.to_numeric(v) for v in value]
        criterion_strategies = {
            MetricsLayerFilterExpressionType.LessThan: lambda f: f < value,
            MetricsLayerFilterExpressionType.LessOrEqualThan: lambda f: f <= value,
            MetricsLayerFilterExpressionType.EqualTo: lambda f: f == value,
            MetricsLayerFilterExpressionType.NotEqualTo: lambda f: f != value,
            MetricsLayerFilterExpressionType.GreaterOrEqualThan: lambda f: f >= value,
            MetricsLayerFilterExpressionType.GreaterThan: lambda f: f > value,
            MetricsLayerFilterExpressionType.Like: lambda f: f.like(value),
            MetricsLayerFilterExpressionType.Contains: lambda f: f.like(f"%{value}%")
            if isinstance(value, str)
            else f.like(value),
            MetricsLayerFilterExpressionType.DoesNotContain: lambda f: f.not_like(f"%{value}%")
            if isinstance(value, str)
            else f.not_like(value),
            MetricsLayerFilterExpressionType.ContainsCaseInsensitive: lambda f: Lower(f).like(
                Lower(f"%{value}%") if isinstance(value, str) else Lower(value)
            ),
            MetricsLayerFilterExpressionType.DoesNotContainCaseInsensitive: lambda f: Lower(f).not_like(
                Lower(f"%{value}%") if isinstance(value, str) else Lower(value)
            ),
            MetricsLayerFilterExpressionType.StartsWith: lambda f: f.like(f"{value}%")
            if isinstance(value, str)
            else f.like(value),
            MetricsLayerFilterExpressionType.EndsWith: lambda f: f.like(f"%{value}")
            if isinstance(value, str)
            else f.like(value),
            MetricsLayerFilterExpressionType.DoesNotStartWith: lambda f: f.not_like(f"{value}%")
            if isinstance(value, str)
            else f.not_like(value),
            MetricsLayerFilterExpressionType.DoesNotEndWith: lambda f: f.not_like(f"%{value}")
            if isinstance(value, str)
            else f.not_like(value),
            MetricsLayerFilterExpressionType.StartsWithCaseInsensitive: lambda f: Lower(f).like(
                Lower(f"{value}%") if isinstance(value, str) else Lower(value)
            ),
            MetricsLayerFilterExpressionType.EndsWithCaseInsensitive: lambda f: Lower(f).like(
                Lower(f"%{value}") if isinstance(value, str) else Lower(value)
            ),
            MetricsLayerFilterExpressionType.DoesNotStartWithCaseInsensitive: lambda f: Lower(f).not_like(
                Lower(f"{value}%") if isinstance(value, str) else Lower(value)
            ),
            MetricsLayerFilterExpressionType.DoesNotEndWithCaseInsensitive: lambda f: Lower(f).not_like(
                Lower(f"%{value}") if isinstance(value, str) else Lower(value)
            ),
            MetricsLayerFilterExpressionType.IsNull: lambda f: f.isnull(),
            MetricsLayerFilterExpressionType.IsNotNull: lambda f: f.notnull(),
            MetricsLayerFilterExpressionType.IsIn: lambda f: f.isin(value),
            MetricsLayerFilterExpressionType.IsNotIn: lambda f: f.isin(value).negate(),
            MetricsLayerFilterExpressionType.BooleanTrue: lambda f: LiteralValueCriterion(f),
            MetricsLayerFilterExpressionType.BooleanFalse: lambda f: f.negate(),
            MetricsLayerFilterExpressionType.IsTrue: lambda f: LiteralValueCriterion(f),
            MetricsLayerFilterExpressionType.IsFalse: lambda f: f.negate(),
        }

        try:
            return criterion_strategies[expression_type](field)
        except KeyError:
            raise QueryError(f"Unknown filter expression_type: {expression_type}.")
