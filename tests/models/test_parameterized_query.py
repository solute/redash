from collections import namedtuple
from unittest import TestCase

import pytest
from mock import patch

from tests import BaseTestCase

from redash.models.parameterized_query import (
    InvalidParameterError,
    ParameterizedQuery,
    QueryDetachedFromDataSourceError,
    dropdown_values,
)

from redash.query_runner import (
    register,
    BaseQueryRunner,
    query_runners,
)

class NoEscapeQueryRunner(BaseQueryRunner):

    @classmethod
    def type(self):
        return "no_escape"


class TestParameterizedQuery(BaseTestCase):

    def setUp(self):
        super().setUp()
        register(NoEscapeQueryRunner)

    def tearDown(self):
        super().tearDown()
        del query_runners["no_escape"]

    def test_returns_empty_list_for_regular_query(self):
        query = ParameterizedQuery("SELECT 1")
        self.assertEqual(set([]), query.missing_params)

    def test_finds_all_params_when_missing(self):
        query = ParameterizedQuery("SELECT {{param}} FROM {{table}}")
        self.assertEqual(set(["param", "table"]), query.missing_params)

    def test_finds_all_params(self):
        query = ParameterizedQuery("SELECT {{param}} FROM {{table}}").apply({"param": "value", "table": "value"})
        self.assertEqual(set([]), query.missing_params)

    def test_deduplicates_params(self):
        query = ParameterizedQuery("SELECT {{param}}, {{param}} FROM {{table}}").apply(
            {"param": "value", "table": "value"}
        )
        self.assertEqual(set([]), query.missing_params)

    def test_handles_nested_params(self):
        query = ParameterizedQuery(
            "SELECT {{param}}, {{param}} FROM {{table}} -- {{#test}} {{nested_param}} {{/test}}"
        ).apply({"param": "value", "table": "value"})
        self.assertEqual(set(["test", "nested_param"]), query.missing_params)

    def _test_optional(self, schema, input_):
        query = ParameterizedQuery(
            "SELECT id, name FROM {{table}}{{#foo}} WHERE foo = {{foo}} {{/foo}};",
            schema=[dict({"name": "foo", "optional": True}, **schema), {"name": "table", "type": "text"}]
        ).apply({"foo": input_, "table": "users"})
        self.assertEqual(query.text, "SELECT id, name FROM users;")

    def test_skips_optional_text_param_with_None(self):
        self._test_optional({"type": "text"}, None)

    def test_skips_optional_number_param_with_None(self):
        self._test_optional({"type": "number"}, None)

    def test_skips_optional_number_param_with_0(self):
        self._test_optional({"type": "number"}, 0)

    def test_skips_optional_date_param_with_None(self):
        self._test_optional({"type": "date"}, None)

    def test_skips_optional_enum_param_with_None(self):
        self._test_optional({"type": "enum"}, None)

    def test_skips_optional_enum_multi_param_with_empty_list(self):
        self._test_optional({"type": "enum", "multiValuesOptions": {}}, [])

    def test_skips_optional_enum_multi_param_with_None(self):
        self._test_optional({"type": "enum", "multiValuesOptions": {}}, None)

    def test_skips_optional_query_param_with_None(self):
        self._test_optional({"type": "query"}, None)

    def test_skips_optional_query_multi_param_with_empty_list(self):
        self._test_optional({"type": "query", "multiValuesOptions": {}}, [])

    def test_skips_optional_query_multi_param_with_None(self):
        self._test_optional({"type": "query", "multiValuesOptions": {}}, None)

    def test_skips_optional_enum_param_with_empty_list_without_multi_values(self):
        with pytest.raises(InvalidParameterError):
            self._test_optional({"type": "enum"}, [])

    def test_skips_optional_query_param_with_empty_list_without_multi_values(self):
        with pytest.raises(InvalidParameterError):
            self._test_optional({"type": "query"}, [])

    @patch(
        "redash.models.parameterized_query.dropdown_values",
        return_value=[{"value": "1"}],
    )
    def test_skips_optional_enum_param_with_empty_string(self, _):
        with pytest.raises(InvalidParameterError) as e:
            self._test_optional({"type": "query"}, "")

    def _test_optional_missing(self, schema):
        query = ParameterizedQuery(
            "SELECT id, name FROM {{table}}{{#foo}} WHERE foo = {{foo}} {{/foo}};",
            schema=[dict({"name": "foo", "optional": True}, **schema), {"name": "table", "type": "text"}]
        ).apply({"table": "users"})
        self.assertEqual(query.text, "SELECT id, name FROM users;")

    def test_skips_optional_text_param_missing(self):
        self._test_optional_missing({"type": "text"})

    def test_skips_optional_number_param_missing(self):
        self._test_optional_missing({"type": "number"})

    def test_skips_optional_number_param_missing(self):
        self._test_optional_missing({"type": "number"})

    def test_skips_optional_date_param_missing(self):
        self._test_optional_missing({"type": "date"})

    def test_skips_optional_enum_param_missing(self):
        self._test_optional_missing({"type": "enum"})

    def test_skips_optional_enum_param_missing(self):
        self._test_optional_missing({"type": "enum", "multiValuesOptions": {}})

    def test_skips_optional_enum_param_missing(self):
        self._test_optional_missing({"type": "query"})

    def test_skips_optional_enum_param_missing(self):
        self._test_optional_missing({"type": "query", "multiValuesOptions": {}})

    def test_raises_on_parameters_not_in_schema(self):
        schema = [{"name": "bar", "type": "text"}]
        query = ParameterizedQuery("foo", schema)

        with pytest.raises(InvalidParameterError):
            query.apply({"qux": 7})

    def test_raises_on_invalid_text_parameters(self):
        schema = [{"name": "bar", "type": "text"}]
        query = ParameterizedQuery("foo", schema)

        with pytest.raises(InvalidParameterError):
            query.apply({"bar": 7})

    @patch("redash.models.parameterized_query._is_number", side_effect=ArithmeticError)
    def test_raises_on_unexpected_validation_error(self, _):
        schema = [{"name": "bar", "type": "number"}]
        query = ParameterizedQuery("foo", schema)

        with pytest.raises(InvalidParameterError):
            query.apply({"bar": 5})

    def test_validates_text_parameters(self):
        schema = [{"name": "bar", "type": "text"}]
        query = ParameterizedQuery("foo {{bar}}", schema)

        query.apply({"bar": "baz"})

        self.assertEqual("foo baz", query.text)

    def test_raises_on_invalid_number_parameters(self):
        schema = [{"name": "bar", "type": "number"}]
        query = ParameterizedQuery("foo", schema)

        with pytest.raises(InvalidParameterError):
            query.apply({"bar": "baz"})

    def test_validates_number_parameters(self):
        schema = [{"name": "bar", "type": "number"}]
        query = ParameterizedQuery("foo {{bar}}", schema)

        query.apply({"bar": 7})

        self.assertEqual("foo 7", query.text)

    def test_coerces_number_parameters(self):
        schema = [{"name": "bar", "type": "number"}]
        query = ParameterizedQuery("foo {{bar}}", schema)

        query.apply({"bar": "3.14"})

        self.assertEqual("foo 3.14", query.text)

    def test_raises_on_invalid_date_parameters(self):
        schema = [{"name": "bar", "type": "date"}]
        query = ParameterizedQuery("foo", schema)

        with pytest.raises(InvalidParameterError):
            query.apply({"bar": "baz"})

    def test_raises_on_none_for_date_parameters(self):
        schema = [{"name": "bar", "type": "date"}]
        query = ParameterizedQuery("foo", schema)

        with pytest.raises(InvalidParameterError):
            query.apply({"bar": None})

    def test_validates_date_parameters(self):
        schema = [{"name": "bar", "type": "date"}]
        query = ParameterizedQuery("foo {{bar}}", schema)

        query.apply({"bar": "2000-01-01 12:00:00"})

        self.assertEqual("foo 2000-01-01 12:00:00", query.text)

    def test_raises_on_invalid_enum_parameters(self):
        schema = [{"name": "bar", "type": "enum", "enumOptions": ["baz", "qux"]}]
        query = ParameterizedQuery("foo", schema)

        with pytest.raises(InvalidParameterError):
            query.apply({"bar": 7})

    def test_raises_on_unlisted_enum_value_parameters(self):
        schema = [{"name": "bar", "type": "enum", "enumOptions": ["baz", "qux"]}]
        query = ParameterizedQuery("foo", schema)

        with pytest.raises(InvalidParameterError):
            query.apply({"bar": "shlomo"})

    def test_raises_on_unlisted_enum_list_value_parameters(self):
        schema = [
            {
                "name": "bar",
                "type": "enum",
                "enumOptions": ["baz", "qux"],
                "multiValuesOptions": {"separator": ",", "prefix": "", "suffix": ""},
            }
        ]
        query = ParameterizedQuery("foo", schema)

        with pytest.raises(InvalidParameterError):
            query.apply({"bar": ["shlomo", "baz"]})

    def test_validates_enum_parameters(self):
        schema = [{"name": "bar", "type": "enum", "enumOptions": ["baz", "qux"]}]
        query = ParameterizedQuery("foo {{bar}}", schema)

        query.apply({"bar": "baz"})

        self.assertEqual("foo baz", query.text)

    def test_validates_enum_list_value_parameters(self):
        schema = [
            {
                "name": "bar",
                "type": "enum",
                "enumOptions": ["baz", "qux"],
                "multiValuesOptions": {"separator": ",", "prefix": "'", "suffix": "'"},
            }
        ]
        query = ParameterizedQuery("foo {{bar}}", schema)

        query.apply({"bar": ["qux", "baz"]})

        assert "foo 'qux','baz'" == query.text

    @patch(
        "redash.models.parameterized_query.dropdown_values",
        return_value=[{"value": "1"}],
    )
    def test_validation_accepts_integer_values_for_dropdowns(self, _):
        schema = [{"name": "bar", "type": "query", "queryId": 1}]
        query = ParameterizedQuery("foo {{bar}}", schema)

        query.apply({"bar": 1})

        self.assertEqual("foo 1", query.text)

    @patch("redash.models.parameterized_query.dropdown_values")
    def test_raises_on_invalid_query_parameters(self, _):
        schema = [{"name": "bar", "type": "query", "queryId": 1}]
        query = ParameterizedQuery("foo", schema)

        with pytest.raises(InvalidParameterError):
            query.apply({"bar": 7})

    @patch(
        "redash.models.parameterized_query.dropdown_values",
        return_value=[{"value": "baz"}],
    )
    def test_raises_on_unlisted_query_value_parameters(self, _):
        schema = [{"name": "bar", "type": "query", "queryId": 1}]
        query = ParameterizedQuery("foo", schema)

        with pytest.raises(InvalidParameterError):
            query.apply({"bar": "shlomo"})

    @patch(
        "redash.models.parameterized_query.dropdown_values",
        return_value=[{"value": "baz"}],
    )
    def test_validates_query_parameters(self, _):
        schema = [{"name": "bar", "type": "query", "queryId": 1}]
        query = ParameterizedQuery("foo {{bar}}", schema)

        query.apply({"bar": "baz"})

        self.assertEqual("foo baz", query.text)

    def test_raises_on_invalid_date_range_parameters(self):
        schema = [{"name": "bar", "type": "date-range"}]
        query = ParameterizedQuery("foo", schema)

        with pytest.raises(InvalidParameterError):
            query.apply({"bar": "baz"})

    def test_validates_date_range_parameters(self):
        schema = [{"name": "bar", "type": "date-range"}]
        query = ParameterizedQuery("foo {{bar.start}} {{bar.end}}", schema)

        query.apply({"bar": {"start": "2000-01-01 12:00:00", "end": "2000-12-31 12:00:00"}})

        self.assertEqual("foo 2000-01-01 12:00:00 2000-12-31 12:00:00", query.text)

    def test_raises_on_unexpected_param_types(self):
        schema = [{"name": "bar", "type": "burrito"}]
        query = ParameterizedQuery("foo", schema)

        with pytest.raises(InvalidParameterError):
            query.apply({"bar": "baz"})

    def test_is_not_safe_if_expecting_text_parameter(self):
        schema = [{"name": "bar", "type": "text"}]
        query = ParameterizedQuery("foo", schema)

        self.assertFalse(query.is_safe)

    def test_is_safe_if_not_expecting_text_parameter(self):
        schema = [{"name": "bar", "type": "number"}]
        query = ParameterizedQuery("foo", schema)

        self.assertTrue(query.is_safe)

    def test_is_safe_if_not_expecting_any_parameters(self):
        schema = []
        query = ParameterizedQuery("foo", schema)

        self.assertTrue(query.is_safe)

    def test_is_safe_if_text_parameter_is_escaped(self):
        schema = [{"name": "bar", "type": "text", "escape": True}]
        query = ParameterizedQuery("foo", schema, org=self.factory.org, data_source=self.factory.data_source)

        self.assertTrue(query.is_safe)

    def test_is_not_safe_if_text_parameter_is_escaped_but_data_source_does_not_support_it(self):
        schema = [{"name": "bar", "type": "text", "escape": True}]
        ds = self.factory.create_data_source(type="no_escape")
        query = ParameterizedQuery("foo", schema, org=self.factory.org, data_source=ds)

        self.assertFalse(query.is_safe)

    @patch(
        "redash.models.parameterized_query._load_result",
        return_value={
            "columns": [{"name": "id"}, {"name": "Name"}, {"name": "Value"}],
            "rows": [{"id": 5, "Name": "John", "Value": "John Doe"}],
        },
    )
    def test_dropdown_values_prefers_name_and_value_columns(self, _):
        values = dropdown_values(1, None)
        self.assertEqual(values, [{"name": "John", "value": "John Doe"}])

    @patch(
        "redash.models.parameterized_query._load_result",
        return_value={
            "columns": [{"name": "id"}, {"name": "fish"}, {"name": "poultry"}],
            "rows": [{"fish": "Clown", "id": 5, "poultry": "Hen"}],
        },
    )
    def test_dropdown_values_compromises_for_first_column(self, _):
        values = dropdown_values(1, None)
        self.assertEqual(values, [{"name": 5, "value": "5"}])

    @patch(
        "redash.models.parameterized_query._load_result",
        return_value={
            "columns": [{"name": "ID"}, {"name": "fish"}, {"name": "poultry"}],
            "rows": [{"fish": "Clown", "ID": 5, "poultry": "Hen"}],
        },
    )
    def test_dropdown_supports_upper_cased_columns(self, _):
        values = dropdown_values(1, None)
        self.assertEqual(values, [{"name": 5, "value": "5"}])

    @patch(
        "redash.models.Query.get_by_id_and_org",
        return_value=namedtuple("Query", "data_source")(None),
    )
    def test_dropdown_values_raises_when_query_is_detached_from_data_source(self, _):
        with pytest.raises(QueryDetachedFromDataSourceError):
            dropdown_values(1, None)
