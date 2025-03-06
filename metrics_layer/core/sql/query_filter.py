import datetime
import json
from typing import Dict

import pandas as pd
from pypika import Criterion, Field, Table
from pypika.terms import LiteralValue

from metrics_layer.core.exceptions import QueryError
from metrics_layer.core.model.base import MetricsLayerBase
from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.model.field import Field as MetricsLayerField
from metrics_layer.core.model.filter import (
    Filter,
    LiteralValueCriterion,
    MetricsLayerFilterExpressionType,
    MetricsLayerFilterGroupLogicalOperatorType,
)
from metrics_layer.core.sql.query_design import MetricsLayerDesign
from metrics_layer.core.sql.query_dialect import query_lookup
from metrics_layer.core.sql.query_errors import ParseError


def datatype_cast(field, value):
    if field.datatype.upper() in {"DATE", "DATETIME"}:
        return LiteralValue(f"CAST(CAST('{value}' AS TIMESTAMP) AS {field.datatype.upper()})")
    return LiteralValue(f"CAST('{value}' AS {field.datatype.upper()})")


class FunnelFilterTypes:
    converted = "converted"
    dropped_off = "dropped_off"


class MetricsLayerFilter(MetricsLayerBase):
    """
    An internal representation of a Filter (WHERE or HAVING clause)
    defined in a MetricsLayerQuery.

    definition: {"field", "expression", "value"}
    """

    def __init__(
        self, definition: Dict = {}, design: MetricsLayerDesign = None, filter_type: str = None, project=None
    ) -> None:
        # The design is used for filters in queries against specific designs
        #  to validate that all the tables and attributes (columns/aggregates)
        #  are properly defined in the design
        self.design = design
        self.project = project
        self.is_literal_filter = "literal" in definition
        # This is a filter with parenthesis like (XYZ or ABC)
        self.is_filter_group = "conditions" in definition

        if self.design:
            self.query_type = self.design.query_type
        else:
            self.query_type = definition["query_type"]
        self.filter_type = filter_type

        self.validate(definition)

        if not self.is_literal_filter and not self.is_filter_group:
            self.expression_type = MetricsLayerFilterExpressionType.parse(definition["expression"])

        super().__init__(definition)

    def __hash__(self):
        valid_def = {
            k: str(v)
            for k, v in self._definition.items()
            if k not in {"group_by_filter_cte_lookup", "query_type", "query_class"}
        }
        return hash(json.dumps(valid_def, sort_keys=True))

    @property
    def conditions(self):
        return self._definition.get("conditions", [])

    @property
    def is_group_by(self):
        return self.group_by is not None or self.expression in {
            MetricsLayerFilterExpressionType.IsInQuery.value,
            MetricsLayerFilterExpressionType.IsNotInQuery.value,
        }

    @property
    def is_funnel(self):
        return self.expression in {FunnelFilterTypes.converted, FunnelFilterTypes.dropped_off}

    def validate(self, definition: Dict) -> None:
        """
        Validate the Filter definition
        """
        key = definition.get("field", None)
        filter_literal = definition.get("literal", None)
        filter_group_conditions = definition.get("conditions", None)

        if filter_group_conditions:
            for f in filter_group_conditions:
                f["query_type"] = self.query_type
                f["group_by_filter_cte_lookup"] = definition.get("group_by_filter_cte_lookup", None)
                MetricsLayerFilter(f, self.design, self.filter_type)

            if (
                "logical_operator" in definition
                and definition["logical_operator"] not in MetricsLayerFilterGroupLogicalOperatorType.options
            ):
                definition.pop("group_by_filter_cte_lookup", None)
                raise ParseError(
                    f"Filter group '{definition}' needs a valid logical operator. Options are:"
                    f" {MetricsLayerFilterGroupLogicalOperatorType.options}"
                )
            return

        is_boolean_value = str(definition.get("value")).lower() == "true" and key is None
        if is_boolean_value:
            definition["value"] = True
        if key is None and filter_literal is None and not is_boolean_value:
            raise ParseError(f"An attribute key or literal was not provided for filter '{definition}'.")

        if key is None and filter_literal:
            return

        if definition["expression"] == "UNKNOWN":
            raise NotImplementedError(f"Unknown filter expression: {definition['expression']}.")

        no_expr = {"is_null", "is_not_null", "boolean_true", "boolean_false", "converted", "dropped_off"}
        if definition.get("value", None) is None and definition["expression"] not in no_expr:
            raise ParseError(f"Filter expression: {definition['expression']} needs a non-empty value.")

        if self.design:
            self.week_start_day = self.design.week_start_day
            self.timezone = self.design.project.timezone
        else:
            self.week_start_day = None
            self.timezone = None

        if self.design and not is_boolean_value:
            # Will raise ParseError if not found
            try:
                self.field = self.design.get_field(key)
            except ParseError:
                raise ParseError(f"We could not find field {self.field_name}")

            # If the value is a string, it might be a field reference.
            # If it is a field reference, we need to replace it with the actual
            # field's sql as a LiteralValue
            # Note: it must be a fully qualified reference, so the '.' is required
            if "value" in definition and isinstance(definition["value"], str) and "." in definition["value"]:
                try:
                    value_field = self.design.get_field(definition["value"])
                    self.design.field_lookup[value_field.id()] = value_field
                    functional_pk = self.design.functional_pk()
                    definition["value"] = LiteralValue(value_field.sql_query(self.query_type, functional_pk))
                except Exception:
                    pass

            if self.design.query_type in Definitions.needs_datetime_cast and self._can_convert_to_datetime(
                definition["value"]
            ):
                definition["value"] = datatype_cast(self.field, definition["value"])

            if self.field.type == "yesno" and "False" in str(definition["value"]):
                definition["expression"] = "boolean_false"

            if self.field.type == "yesno" and "True" in str(definition["value"]):
                definition["expression"] = "boolean_true"

    def _can_convert_to_datetime(self, value):
        if isinstance(value, datetime.datetime):
            return True
        if isinstance(value, str):
            try:
                datetime.datetime.strptime(value, Definitions.date_format_tz)
                return True
            except ValueError:
                return False
        return False

    def group_sql_query(
        self,
        functional_pk: str,
        alias_query: bool = False,
        cte_alias_lookup: dict = {},
        raise_if_not_in_lookup: bool = False,
        field_alias_only: bool = False,
    ):
        pypika_conditions = []
        for condition in self.conditions:
            condition["group_by_filter_cte_lookup"] = self.group_by_filter_cte_lookup
            condition_object = MetricsLayerFilter(condition, self.design, self.filter_type, self.project)
            if condition_object.is_filter_group:
                pypika_conditions.append(
                    condition_object.group_sql_query(
                        functional_pk,
                        alias_query,
                        cte_alias_lookup=cte_alias_lookup,
                        raise_if_not_in_lookup=raise_if_not_in_lookup,
                        field_alias_only=field_alias_only,
                    )
                )
            elif alias_query:
                if self.project is None:
                    raise ValueError("Project is not set, but it is required for an alias_query")
                field_alias = self._handle_cte_alias_replacement(
                    condition_object.field, cte_alias_lookup, raise_if_not_in_lookup
                )
                pypika_conditions.append(condition_object.criterion(field_alias))
            else:
                if condition_object.is_group_by:
                    pypika_conditions.append(condition_object.isin_sql_query())
                elif field_alias_only:
                    pypika_conditions.append(
                        condition_object.criterion(condition_object.field.alias(with_view=True))
                    )
                else:
                    pypika_conditions.append(
                        condition_object.criterion(
                            condition_object.field.sql_query(self.query_type, functional_pk)
                        )
                    )
        if self.logical_operator == MetricsLayerFilterGroupLogicalOperatorType.or_:
            return Criterion.any(pypika_conditions)
        if (
            self.logical_operator is None
            or self.logical_operator == MetricsLayerFilterGroupLogicalOperatorType.and_
        ):
            return Criterion.all(pypika_conditions)
        raise ParseError(f"Invalid logical operator: {self.logical_operator}")

    def sql_query(
        self,
        alias_query: bool = False,
        cte_alias_lookup: dict = {},
        raise_if_not_in_lookup: bool = False,
        field_alias_only: bool = False,
    ):
        if self.is_literal_filter:
            return LiteralValueCriterion(self.replace_fields_literal_filter())

        if alias_query and self.is_filter_group:
            return self.group_sql_query("NA", alias_query, cte_alias_lookup, raise_if_not_in_lookup)
        elif alias_query:
            field_alias = self._handle_cte_alias_replacement(
                self.field, cte_alias_lookup, raise_if_not_in_lookup
            )
            return self.criterion(field_alias)

        functional_pk = self.design.functional_pk()
        if self.is_filter_group:
            return self.group_sql_query(functional_pk)
        elif self.is_group_by:
            return self.isin_sql_query()
        elif field_alias_only:
            return self.criterion(self.field.alias(with_view=True))
        else:
            return self.criterion(self.field.sql_query(self.query_type, functional_pk))

    def _handle_cte_alias_replacement(
        self, field_id: str, cte_alias_lookup: dict, raise_if_not_in_lookup: bool
    ):
        field = self.project.get_field(field_id)
        field_alias = field.alias(with_view=True)
        if field_alias in cte_alias_lookup:
            field_alias = f"{cte_alias_lookup[field_alias]}.{field_alias}"
        elif raise_if_not_in_lookup:
            self._raise_query_error_from_cte(field.id())
        return field_alias

    def isin_sql_query(self):
        cte_alias = self.group_by_filter_cte_lookup[hash(self)]
        if self.group_by:
            return self._create_legacy_group_by_is_in_query(cte_alias)
        else:
            return self._create_is_in_query(cte_alias)

    def _create_is_in_query(self, cte_alias):
        connection_field_id = self.value["field"]
        connection_field = self.design.get_field(connection_field_id)
        base = query_lookup[self.query_type]
        subquery = base.from_(Table(cte_alias)).select(connection_field.alias(with_view=True)).distinct()
        if self.expression == MetricsLayerFilterExpressionType.IsNotInQuery.value:
            expression = MetricsLayerFilterExpressionType.IsNotIn.value
        elif self.expression == MetricsLayerFilterExpressionType.IsInQuery.value:
            expression = MetricsLayerFilterExpressionType.IsIn.value
        else:
            raise QueryError(f"Invalid expression for subquery filter: {self.expression}")

        definition = {
            "query_type": self.query_type,
            "field": self.field.id(),
            "expression": expression,
            "value": subquery,
        }
        f = MetricsLayerFilter(definition=definition, design=None, filter_type="where")
        return f.criterion(self.field.sql_query(self.query_type))

    def _create_legacy_group_by_is_in_query(self, cte_alias):
        group_by_field = self.design.get_field(self.group_by)
        base = query_lookup[self.query_type]
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
        if self.filter_type == "where":
            extra_args = {"field_type": None}
        else:
            extra_args = {"field_type": "measure", "type": "number"}
        view = self.design.get_view(self.design.base_view_name)
        field = MetricsLayerField({"sql": self.literal, "name": None, **extra_args}, view=view)
        return field.sql_query(self.query_type, functional_pk=self.design.functional_pk())

    def criterion(self, field_sql: str) -> Criterion:
        """
        Generate the Pypika Criterion for this filter

        We have to use the following cases as PyPika does not allow an str
         representation of the clause on its where() and having() functions
        """
        if self.expression_type == MetricsLayerFilterExpressionType.Matches:
            criteria = []
            filter_dict = {
                "field": self.field.alias(),
                "value": self.value,
                "week_start_day": self.week_start_day,
                "timezone": self.timezone,
            }
            for f in Filter(filter_dict).filter_dict():
                if self.query_type in Definitions.needs_datetime_cast:
                    value = datatype_cast(self.field, f["value"])
                else:
                    value = f["value"]
                criteria.append(Filter.sql_query(field_sql, f["expression"], value, self.field.type))
            return Criterion.all(criteria)
        if isinstance(self.field, MetricsLayerField):
            field_datatype = self.field.type
        else:
            field_datatype = "unknown"
        return Filter.sql_query(field_sql, self.expression_type, self.value, field_datatype)

    def cte(self, query_class, design_class):
        if not self.is_group_by:
            raise QueryError(
                "A CTE is invalid for a filter with no group_by property or is_in_query/is_not_in_query"
                " expression"
            )
        if self.group_by:
            return self._create_subquery_from_group_by_property(query_class, design_class)
        elif self.expression in {
            MetricsLayerFilterExpressionType.IsInQuery.value,
            MetricsLayerFilterExpressionType.IsNotInQuery.value,
        }:
            return self._create_subquery_from_query_property()
        else:
            raise QueryError(
                "A CTE is invalid for a filter with no group_by property or is_in_query/is_not_in_query"
                " expression"
            )

    def _create_subquery_from_query_property(self):
        # This is a subquery that's compiled in the `resolve.py` file in the initial parsing step.
        return self.value["sql_query"]

    def _create_subquery_from_group_by_property(self, query_class, design_class):
        group_by_filters = [{k: v for k, v in self._definition.items() if k != "group_by"}]

        field_lookup = {}
        group_by_field = self.design.get_field(self.group_by)
        field_lookup[group_by_field.id()] = group_by_field

        filter_dict_args = {"where": [], "having": []}
        for group_by_filter in group_by_filters:
            filter_field = self.design.get_field(group_by_filter["field"])
            field_lookup[filter_field.id()] = filter_field

            if filter_field.field_type == "measure":
                filter_dict_args["having"].append(group_by_filter)
            else:
                filter_dict_args["where"].append(group_by_filter)

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
            **filter_dict_args,
            "return_pypika_query": True,
        }
        generator = query_class(config, design=design)
        return generator.get_query()

    def funnel_cte(self):
        if not self.is_funnel:
            raise QueryError("A funnel CTE is invalid for a filter with no funnel property")

        _from, _to = self._definition["from"], self._definition["to"]
        from_cte, to_cte = self.query_class._cte(_from), self.query_class._cte(_to)

        base_query = self.query_class._base_query()
        base_table = Table(self.query_class.base_cte_name)
        base_query = base_query.from_(base_table).select(self.query_class.link_alias)

        from_cond = self.__funnel_in_step(from_cte, isin=True)
        converted = self.expression == FunnelFilterTypes.converted
        to_cond = self.__funnel_in_step(to_cte, isin=converted)

        base_query = base_query.where(Criterion.all([from_cond, to_cond])).distinct()
        return base_query

    def __funnel_in_step(self, step_cte: str, isin: bool):
        base_query = self.query_class._base_query()
        field = Field(self.query_class.link_alias)
        subquery = base_query.from_(Table(step_cte)).select(self.query_class.link_alias).distinct()
        if isin:
            return field.isin(subquery)
        return field.isin(subquery).negate()
