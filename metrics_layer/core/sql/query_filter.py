import datetime
from copy import deepcopy
from typing import Dict

import sqlparse
from pypika import Criterion, Table
from pypika.terms import LiteralValue
from sqlparse.tokens import Error, Name, Punctuation
from metrics_layer.core.exceptions import QueryError

from metrics_layer.core.model.base import MetricsLayerBase
from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.model.field import Field as MetricsLayerField
from metrics_layer.core.model.filter import Filter, MetricsLayerFilterExpressionType, LiteralValueCriterion
from metrics_layer.core.sql.query_design import MetricsLayerDesign
from metrics_layer.core.sql.query_errors import ParseError


def bigquery_cast(field, value):
    cast_func = field.datatype.upper()
    return LiteralValue(f"{cast_func}('{value}')")


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
        if self.design:
            self.query_type = self.design.query_type
        else:
            self.query_type = definition["query_type"]
        self.filter_type = filter_type

        self.validate(definition)

        if not self.is_literal_filter:
            self.expression_type = MetricsLayerFilterExpressionType.parse(definition["expression"])

        super().__init__(definition)

    @property
    def is_group_by(self):
        return self.group_by is not None

    def validate(self, definition: Dict) -> None:
        """
        Validate the Filter definition
        """
        key = definition.get("field", None)
        filter_literal = definition.get("literal", None)

        is_boolean_value = str(definition.get("value")).lower() == "true" and key is None
        if is_boolean_value:
            definition["value"] = True
        if key is None and filter_literal is None and not is_boolean_value:
            raise ParseError(f"An attribute key or literal was not provided for filter '{definition}'.")

        if key is None and filter_literal:
            return

        if definition["expression"] == "UNKNOWN":
            raise NotImplementedError(f"Unknown filter expression: {definition['expression']}.")

        no_expr = {"is_null", "is_not_null", "boolean_true", "boolean_false"}
        if definition.get("value", None) is None and definition["expression"] not in no_expr:
            raise ParseError(f"Filter expression: {definition['expression']} needs a non-empty value.")

        if self.design and not is_boolean_value:
            # Will raise ParseError if not found
            try:
                self.field = self.design.get_field(key)
            except ParseError:
                raise ParseError(
                    f"We could not find field {self.field_name} in explore {self.design.explore.name}"
                )

            if self.design.query_type == "BIGQUERY" and isinstance(definition["value"], datetime.datetime):
                definition["value"] = bigquery_cast(self.field, definition["value"])

            if self.field.type == "yesno" and "False" in str(definition["value"]):
                definition["expression"] = "boolean_false"

            if self.field.type == "yesno" and "True" in str(definition["value"]):
                definition["expression"] = "boolean_true"

    def sql_query(self):
        if self.is_literal_filter:
            return LiteralValueCriterion(self.replace_fields_literal_filter())
        functional_pk = self.design.functional_pk()
        return self.criterion(self.field.sql_query(self.query_type, functional_pk))

    def group_by_sql_query(self, cte_alias, query_generator):
        group_by_field = self.design.get_field(self.group_by)
        base = query_generator._base_query()
        subquery = base.from_(Table(cte_alias)).select(group_by_field.alias(with_view=True)).distinct()
        definition = {
            "query_type": self.query_type,
            "field": self.group_by,
            "expression": MetricsLayerFilterExpressionType.IsIn.value,
            "value": subquery,
        }
        f = MetricsLayerFilter(definition=definition, design=None, filter_type="where")
        return f.criterion(group_by_field.sql_query(self.query_type))

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
                    if (len(generator) - i - 1) >= 2 and generator[i + 2].ttype != Name:
                        tokens.append(str(token))
            elif token.ttype == Name and not not_already_added:
                pass
            elif token.ttype == Punctuation and str(token) == ".":
                if generator[i - 1].ttype == Name and generator[i + 1].ttype == Name:
                    field = self.design.get_field(f"{field_names[-1]}.{str(generator[i+1])}")
                    tokens.append("${" + field.view.name + "." + str(generator[i + 1]) + "}")
            elif token.ttype != Error:
                tokens.append(str(token))

        return tokens

    def criterion(self, field_sql: str) -> Criterion:
        """
        Generate the Pypika Criterion for this filter

        We have to use the following cases as PyPika does not allow an str
         representation of the clause on its where() and having() functions
        """
        if self.expression_type == MetricsLayerFilterExpressionType.Matches:
            criteria = []
            for f in Filter({"field": self.field.alias(), "value": self.value}).filter_dict():
                if self.query_type == Definitions.bigquery:
                    value = bigquery_cast(self.field, f["value"])
                else:
                    value = f["value"]
                criteria.append(Filter.sql_query(field_sql, f["expression"], value))
            return Criterion.all(criteria)
        return Filter.sql_query(field_sql, self.expression_type, self.value)

    def cte(self, query_class, design_class):
        if not self.is_group_by:
            raise QueryError("A CTE is invalid for a filter with no group_by property")

        having_filter = {k: v for k, v in self._definition.items() if k != "group_by"}
        field_names = [self.group_by, having_filter["field"]]
        field_lookup = {}
        for n in field_names:
            field = self.design.get_field(n)
            field_lookup[field.id()] = field

        design = design_class(
            no_group_by=False,
            query_type=self.design.query_type,
            field_lookup=field_lookup,
            model=self.design.model,
            project=self.design.project,
        )

        config = {
            "metrics": [],
            "dimensions": [self.group_by],
            "having": [having_filter],
            "return_pypika_query": True,
        }
        generator = query_class(config, design=design)
        return generator.get_query()
