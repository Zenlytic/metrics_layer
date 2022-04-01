import sqlparse
from sqlparse.tokens import Name, Punctuation

from metrics_layer.core.parse.config import ConfigError, MetricsLayerConfiguration
from metrics_layer.core.sql.query_design import MetricsLayerDesign
from metrics_layer.core.sql.query_generator import MetricsLayerQuery


class SingleSQLQueryResolver:
    """
    Method of resolving the explore name:
        if there is not explore passed (using the format explore_name.field_name), we'll search for
        just the field name and iff that field is used in only one explore, set that as the active explore.
            - Any fields specified that are not in that explore will raise an error

        if it's passed explicitly, use the first metric's explore, and raise an error if anything conflicts
        with that explore
    """

    def __init__(
        self,
        metrics: list,
        dimensions: list = [],
        where: str = None,  # Either a list of json or a string
        having: str = None,  # Either a list of json or a string
        order_by: str = None,  # Either a list of json or a string
        config: MetricsLayerConfiguration = None,
        **kwargs,
    ):
        self.field_lookup = {}
        self.no_group_by = False
        self.verbose = kwargs.get("verbose", False)
        self.select_raw_sql = kwargs.get("select_raw_sql", [])
        self.explore_name = kwargs.get("explore_name")
        self.suppress_warnings = kwargs.get("suppress_warnings", False)
        self.limit = kwargs.get("limit")
        self.return_pypika_query = kwargs.get("return_pypika_query")
        self.config = config
        self.project = self.config.project
        self.metrics = metrics
        self.dimensions = dimensions
        self.parse_field_names(where, having, order_by)

        if not self.explore_name:
            self.explore_name = self.derive_explore(self.verbose)

        self.explore = self.project.get_explore(self.explore_name)
        try:
            self.connection = self.config.get_connection(self.explore.model.connection)
        except ConfigError:
            self.connection = None

        if "query_type" in kwargs:
            self.query_type = kwargs["query_type"]
        elif self.connection:
            self.query_type = self.connection.type
        else:
            raise ConfigError(
                "Could not determine query_type. Please have connection information for "
                "your warehouse in the configuration or explicitly pass the "
                "'query_type' argument to this function"
            )
        self.parse_input()

    def get_query(self, semicolon: bool = True):
        self.design = MetricsLayerDesign(
            no_group_by=self.no_group_by,
            query_type=self.query_type,
            field_lookup=self.field_lookup,
            explore=self.explore,
            project=self.project,
        )

        query_definition = {
            "metrics": self.metrics,
            "dimensions": self.dimensions,
            "where": self.where,
            "having": self.having,
            "order_by": self.order_by,
            "select_raw_sql": self.select_raw_sql,
            "limit": self.limit,
            "return_pypika_query": self.return_pypika_query,
        }
        query = MetricsLayerQuery(
            query_definition, design=self.design, suppress_warnings=self.suppress_warnings
        ).get_query(semicolon=semicolon)

        return query

    def derive_explore(self, verbose: bool):
        # Only checking metrics when they exist reduces the number of obvious explores a user has to specify
        if len(self.metrics) > 0:
            all_fields = self.metrics
        else:
            all_fields = self.dimensions

        if len(all_fields) == 0:
            raise ValueError("You need to include at least one metric or dimension for the query to run")

        initial_field = all_fields[0]
        working_explore_name = self.project.get_explore_from_field(initial_field)
        for field_name in all_fields[1:]:
            explore_name = self.project.get_explore_from_field(field_name)
            if explore_name != working_explore_name:
                raise ValueError(
                    f"""The explore found in metric {initial_field}, {working_explore_name}
                    does not match the explore found in {field_name}, {explore_name}"""
                )

        if verbose:
            print(f"Setting query explore to: {working_explore_name}")
        return working_explore_name

    def parse_input(self):
        # TODO handle this case in the future
        if self.explore.symmetric_aggregates == "no":
            raise NotImplementedError(
                "MetricsLayer does not currently support turning off symmetric aggregates"
            )

        all_field_names = self.metrics + self.dimensions
        if len(set(all_field_names)) != len(all_field_names):
            # TODO improve this error message
            raise ValueError("Ambiguous field names in the metrics and dimensions")

        for name in self.metrics:
            self.field_lookup[name] = self.get_field_with_error_handling(name, "Metric")

        # Dimensions exceptions:
        #   They are coming from a different explore than the metric, not joinable (handled in get_field)
        #   They are not found in the selected explore (handled here)

        for name in self.dimensions:
            field = self.get_field_with_error_handling(name, "Dimension")
            # We will not use a group by if the primary key of the main resulting table is included
            if field.primary_key == "yes" and field.view.name == self.explore.from_:
                self.no_group_by = True
            self.field_lookup[name] = field

        for name in self._where_field_names:
            self.field_lookup[name] = self.get_field_with_error_handling(name, "Where clause field")

        for name in self._having_field_names:
            self.field_lookup[name] = self.get_field_with_error_handling(name, "Having clause field")

        for name in self._order_by_field_names:
            self.field_lookup[name] = self.get_field_with_error_handling(name, "Order by field")

    def get_field_with_error_handling(self, field_name: str, error_prefix: str):
        field = self.project.get_field(field_name, explore_name=self.explore_name)
        if field is None:
            raise ValueError(f"{error_prefix} {field_name} not found in explore {self.explore_name}")
        return field

    def parse_field_names(self, where, having, order_by):
        self.where = self._check_for_dict(where)
        if self._is_literal(self.where):
            self._where_field_names = self.parse_identifiers_from_clause(self.where)
        else:
            self._where_field_names = self.parse_identifiers_from_dicts(self.where)

        self.having = self._check_for_dict(having)
        if self._is_literal(self.having):
            self._having_field_names = self.parse_identifiers_from_clause(self.having)
        else:
            self._having_field_names = self.parse_identifiers_from_dicts(self.having)

        self.order_by = self._check_for_dict(order_by)
        if self._is_literal(self.order_by):
            self._order_by_field_names = self.parse_identifiers_from_clause(self.order_by)
        else:
            self._order_by_field_names = self.parse_identifiers_from_dicts(self.order_by)

    @staticmethod
    def _is_literal(clause):
        return isinstance(clause, str) or clause is None

    @staticmethod
    def parse_identifiers_from_clause(clause: str):
        if clause is None:
            return []
        generator = list(sqlparse.parse(clause)[0].flatten())

        field_names = []
        for i, token in enumerate(generator):
            not_already_added = i == 0 or str(generator[i - 1]) != "."
            if token.ttype == Name and not_already_added:
                field_names.append(str(token))

            if token.ttype == Punctuation and str(token) == ".":
                if generator[i - 1].ttype == Name and generator[i + 1].ttype == Name:
                    field_names[-1] += f".{str(generator[i+1])}"
        return field_names

    @staticmethod
    def parse_identifiers_from_dicts(conditions: list):
        try:
            return [cond["field"] for cond in conditions]
        except KeyError:
            for cond in conditions:
                if "field" not in cond:
                    break
            raise KeyError(f"Identifier was missing required 'field' key: {cond}")

    @staticmethod
    def _check_for_dict(conditions: list):
        if isinstance(conditions, dict):
            return [conditions]
        return conditions
