import datetime
from copy import deepcopy
from typing import Dict

import sqlparse
from pypika import Criterion, Field
from pypika.terms import LiteralValue
from sqlparse.tokens import Error, Name, Punctuation

from metrics_layer.core.model.base import MetricsLayerBase
from metrics_layer.core.model.field import Field as MetricsLayerField
from metrics_layer.core.model.filter import MetricsLayerFilterExpressionType
from metrics_layer.core.sql.pypika_types import LiteralValueCriterion
from metrics_layer.core.sql.query_design import MetricsLayerDesign
from metrics_layer.core.sql.query_errors import ParseError


class MetricsLayerFilter(MetricsLayerBase):
    """
    An internal representation of a Filter (WHERE or HAVING clause)
    defined in a MetricsLayerQuery.

    definition: {"field", "expression", "value"}
    """

    def __init__(
        self, definition: Dict = {}, design: MetricsLayerDesign = None, filter_type: str = None
    ) -> None:
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
            self.expression_type = MetricsLayerFilterExpressionType.parse(definition["expression"])

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
        functional_pk = self.design.functional_pk()
        return self.criterion(LiteralValue(self.field.sql_query(self.query_type, functional_pk)))

    def replace_fields_literal_filter(self):
        tokens = self._parse_sql_literal(self.literal)

        if self.filter_type == "where":
            extra_args = {"field_type": None}
        else:
            extra_args = {"field_type": "measure", "type": "number"}
        view = self.design.get_view(self.design.base_view_name)
        field = MetricsLayerField({"sql": "".join(tokens), "name": None, **extra_args}, view=view)
        return field.sql_query(self.query_type, functional_pk=self.design.functional_pk())

    def _parse_sql_literal(self, clause: str):
        generator = list(sqlparse.parse(clause)[0].flatten())
        tokens, field_names = [], []
        for i, token in enumerate(generator):
            not_already_added = i == 0 or str(generator[i - 1]) != "."

            if token.ttype == Name and not_already_added:
                try:
                    field = self.design.get_field(str(token))
                    tokens.append("${" + field.view.name + "." + str(token) + "}")
                except Exception:
                    field_names.append(str(token))
            elif token.ttype == Name and not not_already_added:
                pass
            elif token.ttype == Punctuation and str(token) == ".":
                if generator[i - 1].ttype == Name and generator[i + 1].ttype == Name:
                    field = self.design.get_field(f"{field_names[-1]}.{str(generator[i+1])}")
                    tokens.append("${" + field.view.name + "." + str(generator[i + 1]) + "}")
            elif token.ttype != Error:
                tokens.append(str(token))

        return tokens

    def criterion(self, field: Field) -> Criterion:
        """
        Generate the Pypika Criterion for this filter

        field: Pypika Field as we do not know the base table this filter is
                evaluated against (is it the base table or an intermediary table?)

        We have to use the following cases as PyPika does not allow an str
         representation of the clause on its where() and having() functions
        """
        criterion_strategies = {
            MetricsLayerFilterExpressionType.LessThan: lambda f: f < self.value,
            MetricsLayerFilterExpressionType.LessOrEqualThan: lambda f: f <= self.value,
            MetricsLayerFilterExpressionType.EqualTo: lambda f: f == self.value,
            MetricsLayerFilterExpressionType.NotEqualTo: lambda f: f != self.value,
            MetricsLayerFilterExpressionType.GreaterOrEqualThan: lambda f: f >= self.value,
            MetricsLayerFilterExpressionType.GreaterThan: lambda f: f > self.value,
            MetricsLayerFilterExpressionType.Like: lambda f: f.like(self.value),
            MetricsLayerFilterExpressionType.Contains: lambda f: f.like(f"%{self.value}%"),
            MetricsLayerFilterExpressionType.DoesNotContain: lambda f: f.not_like(f"%{self.value}%"),
            MetricsLayerFilterExpressionType.ContainsCaseInsensitive: lambda f: f.ilike(f"%{self.value}%"),
            MetricsLayerFilterExpressionType.DoesNotContainCaseInsensitive: lambda f: f.not_ilike(
                f"%{self.value}%"
            ),
            MetricsLayerFilterExpressionType.StartsWith: lambda f: f.like(f"{self.value}%"),
            MetricsLayerFilterExpressionType.EndsWith: lambda f: f.like(f"%{self.value}"),
            MetricsLayerFilterExpressionType.DoesNotStartWith: lambda f: f.not_like(f"{self.value}%"),
            MetricsLayerFilterExpressionType.DoesNotEndWith: lambda f: f.not_like(f"%{self.value}"),
            MetricsLayerFilterExpressionType.StartsWithCaseInsensitive: lambda f: f.ilike(f"{self.value}%"),
            MetricsLayerFilterExpressionType.EndsWithCaseInsensitive: lambda f: f.ilike(f"%{self.value}"),
            MetricsLayerFilterExpressionType.DoesNotStartWithCaseInsensitive: lambda f: f.not_ilike(
                f"{self.value}%"
            ),
            MetricsLayerFilterExpressionType.DoesNotEndWithCaseInsensitive: lambda f: f.not_ilike(
                f"%{self.value}"
            ),
            MetricsLayerFilterExpressionType.IsNull: lambda f: f.isnull(),
            MetricsLayerFilterExpressionType.IsNotNull: lambda f: f.notnull(),
            MetricsLayerFilterExpressionType.IsIn: lambda f: f.isin(self.value),
            MetricsLayerFilterExpressionType.IsNotIn: lambda f: f.isin(self.value).negate(),
            MetricsLayerFilterExpressionType.BooleanTrue: lambda f: f,
            MetricsLayerFilterExpressionType.BooleanFalse: lambda f: f.negate(),
        }

        try:
            return criterion_strategies[self.expression_type](field)
        except KeyError:
            raise NotImplementedError(f"Unknown filter expression_type: {self.expression_type}.")
