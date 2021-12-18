import logging
from functools import partial
from numbers import Number

import pystache
from dateutil.parser import parse
from funcy import distinct

from redash.utils import mustache_render


logger = logging.getLogger(__name__)


def _pluck_name_and_value(default_column, row):
    row = {k.lower(): v for k, v in row.items()}
    name_column = "name" if "name" in row.keys() else default_column.lower()
    value_column = "value" if "value" in row.keys() else default_column.lower()

    return {"name": row[name_column], "value": str(row[value_column])}


def _load_result(query_id, org):
    from redash import models

    query = models.Query.get_by_id_and_org(query_id, org)

    if query.data_source:
        query_result = models.QueryResult.get_by_id_and_org(query.latest_query_data_id, org)
        return query_result.data
    else:
        raise QueryDetachedFromDataSourceError(query_id)


def dropdown_values(query_id, org):
    data = _load_result(query_id, org)
    first_column = data["columns"][0]["name"]
    pluck = partial(_pluck_name_and_value, first_column)
    return list(map(pluck, data["rows"]))


def _join_list_values(definition, values, data_source):
    multi_values_options = definition.get("multiValuesOptions", {})
    separator = str(multi_values_options.get("separator", ","))
    prefix = str(multi_values_options.get("prefix", ""))
    suffix = str(multi_values_options.get("suffix", ""))
    return separator.join(prefix + _maybe_escape(definition, v, data_source) + suffix for v in values)


def _collect_key_names(nodes):
    keys = []
    for node in nodes._parse_tree:
        if isinstance(node, pystache.parser._EscapeNode):
            keys.append(node.key)
        elif isinstance(node, pystache.parser._SectionNode):
            keys.append(node.key)
            keys.extend(_collect_key_names(node.parsed))

    return distinct(keys)


def _collect_query_parameters(query):
    nodes = pystache.parse(query)
    keys = _collect_key_names(nodes)
    return keys


def _parameter_names(parameter_values):
    names = []
    for key, value in parameter_values.items():
        if isinstance(value, dict):
            for inner_key in value.keys():
                names.append("{}.{}".format(key, inner_key))
        else:
            names.append(key)

    return names


def _maybe_escape(definition, value, data_source):
    if definition.get("escape") and data_source.query_runner.supports_escape:
        value = data_source.query_runner.escape_parameter(value)
    return value


def _handle_text(definition, value, data_source):
    if value is None and definition.get("optional"):
        return None

    if not isinstance(value, str):
        raise ValueError("Not a string: {!r}".format(value))

    return _maybe_escape(definition, value, data_source)


def _handle_number(definition, value, data_source):
    if value is None and definition.get("optional"):
        return None

    try:
        if isinstance(value, Number):
            return value
        return float(value)
    except (ValueError, TypeError):
        raise ValueError("Could not parse float from value: {!r}".format(value))


def _handle_date(definition, value, data_source):
    if value is None and definition.get("optional"):
        return None

    try:
        parse(value)
        return value
    except (ValueError, TypeError):
        raise ValueError("Could not parse date from value {!r}".format(value))


def _handle_date_range(definition, obj, data_source):
    if obj is None and definition.get("optional"):
        return {"start": None, "end": None}

    if not isinstance(obj, dict) or not "start" in obj or not "end" in obj:
        raise ValueError("Mismatched date range format, need dict with start and end: {!r}".format(obj))

    return {
        "start":_handle_date(definition, obj["start"], data_source),
        "end": _handle_date(definition, obj["end"], data_source),
    }


def _handle_options(options_getter):
    def _handle_options_wrapper(definition, value, data_source):
        allow_multiple_values = isinstance(definition.get("multiValuesOptions"), dict)
        optional = definition.get("optional")

        if isinstance(value, list):
            if not allow_multiple_values:
                raise ValueError("Multi values not allowed, got {!r}".format(value))

            if optional and not value:
                return None

            values = [str(_) for _ in value]

            options = set(options_getter(definition, value))
            if not set(values).issubset(options):
                raise ValueError("Got invalid values for enum {!r}".format(set(values).difference(options)))

            return _join_list_values(definition, values, data_source)
        else:
            if optional and value is None:
                return None

            value = str(value)
            options = set(options_getter(definition, value))
            if not str(value) in options:
                raise ValueError("Got invalid value for enum {!r}".format(value))

            return _maybe_escape(definition, value, data_source)
    return _handle_options_wrapper


class ParameterizedQuery(object):
    def __init__(self, template, schema=None, org=None, data_source=None):
        self.schema = schema or []
        self.org = org
        self.data_source = data_source
        self.template = template
        self.query = template
        self.parameters = {}

    def apply(self, parameters):
        invalid_parameter_names = []
        parameters = dict(parameters)
        for name, value in parameters.items():
            try:
                parameters[name] = self._handle(name, value)
            except (ValueError, TypeError):
                logger.warning("Failed parameter validation", exc_info=True)
                invalid_parameter_names.append(name)

        if invalid_parameter_names:
            raise InvalidParameterError(invalid_parameter_names)
        else:
            self.parameters.update(parameters)
            self.query = mustache_render(self.template, parameters)

        return self

    def _handle(self, name, value):
        if not self.schema:
            return value

        definition = next(
            (definition for definition in self.schema if definition["name"] == name),
            None,
        )

        if not definition:
            raise ValueError("No definition found for parameter: {!r}".format(name))

        def get_enum_options(definition, value):
            options = definition.get("enumOptions")
            if isinstance(options, str):
                options = options.split("\n")
            return options

        def get_query_options(definition, value):
            return [v["value"] for v in dropdown_values(definition.get("queryId"), self.org)]

        handlers = {
            "text": _handle_text,
            "number": _handle_number,
            "enum": _handle_options(get_enum_options),
            "query": _handle_options(get_query_options),
            "date": _handle_date,
            "datetime-local": _handle_date,
            "datetime-with-seconds": _handle_date,
            "date-range": _handle_date_range,
            "datetime-range": _handle_date_range,
            "datetime-range-with-seconds": _handle_date_range,
        }

        handle = handlers.get(definition["type"])

        if not handle:
            raise TypeError("Unknown parameter type: {!r}".format(definition["type"]))

        return handle(definition, value, self.data_source)

    @property
    def is_safe(self):
        text_parameters = [
            param
            for param in self.schema if (
                param["type"] == "text"
                and (
                    self.data_source is None
                    or not self.data_source.query_runner.supports_escape
                    or not param.get("escape")
                )
            )
        ]
        return not any(text_parameters)

    @property
    def missing_params(self):
        query_parameters = set(_collect_query_parameters(self.template))
        return set(query_parameters) - set(_parameter_names(self.parameters))

    @property
    def text(self):
        return self.query


class InvalidParameterError(Exception):
    def __init__(self, parameters):
        parameter_names = ", ".join(parameters)
        message = "The following parameter values are incompatible with their definitions: {}".format(parameter_names)
        super(InvalidParameterError, self).__init__(message)


class QueryDetachedFromDataSourceError(Exception):
    def __init__(self, query_id):
        self.query_id = query_id
        super(QueryDetachedFromDataSourceError, self).__init__(
            "This query is detached from any data source. Please select a different query."
        )
