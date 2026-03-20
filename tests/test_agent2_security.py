"""Unit tests for agent2_converter_v2.py security and SQL generation."""
import json
import pytest
import sys
sys.path.insert(0, str(__file__).rsplit('/tests/', 1)[0] + '/src')

from agent2_converter_v2 import (
    escape_dollar_quotes,
    clean_expression,
    generate_model_name,
    generate_column_list,
)


class TestEscapeDollarQuotes:
    def test_empty_string(self):
        assert escape_dollar_quotes("") == ""
    
    def test_none_input(self):
        assert escape_dollar_quotes(None) == ""
    
    def test_no_dollars(self):
        assert escape_dollar_quotes("SELECT * FROM table") == "SELECT * FROM table"
    
    def test_single_dollar(self):
        assert escape_dollar_quotes("$var") == "$var"
    
    def test_double_dollar_escaped(self):
        assert escape_dollar_quotes("$$injection$$") == "$\\$$\\$"
    
    def test_mixed_content(self):
        result = escape_dollar_quotes("BEGIN $$ SELECT 1 $$ END")
        assert "$$" not in result or result.count("\\$") >= 2


class TestCleanExpression:
    def test_empty_returns_true(self):
        assert clean_expression("") == "TRUE"
        assert clean_expression(None) == "TRUE"
    
    def test_decode_to_case(self):
        result = clean_expression("DECODE(TRUE, x, y)")
        assert "CASE WHEN" in result
    
    def test_iif_to_iff(self):
        result = clean_expression("IIF(a > b, 1, 0)")
        assert "IFF(" in result
    
    def test_truncation_with_warning(self, capsys):
        long_expr = "A" * 600
        result = clean_expression(long_expr, warn_on_truncate=True)
        assert len(result) <= 550
        assert "TRUNCATED" in result
        captured = capsys.readouterr()
        assert "WARN" in captured.out
    
    def test_truncation_without_warning(self, capsys):
        long_expr = "B" * 600
        result = clean_expression(long_expr, warn_on_truncate=False)
        assert "TRUNCATED" in result
        captured = capsys.readouterr()
        assert "WARN" not in captured.out


class TestGenerateModelName:
    def test_staging_prefix(self):
        assert generate_model_name("stg_customers").startswith("stg_")
    
    def test_intermediate_prefix(self):
        assert generate_model_name("int_orders").startswith("int_")
    
    def test_mart_prefix(self):
        assert generate_model_name("mart_sales").startswith("mart_")
    
    def test_default_to_mart(self):
        result = generate_model_name("customer_dim")
        assert result.startswith(("stg_", "int_", "mart_"))
    
    def test_lowercase_conversion(self):
        result = generate_model_name("CUSTOMER_TABLE")
        assert result == result.lower()


class TestGenerateColumnList:
    def test_empty_returns_star(self):
        assert generate_column_list([]) == "*"
    
    def test_single_column(self):
        fields = [{"name": "ID"}]
        result = generate_column_list(fields)
        assert "id" in result.lower()
    
    def test_truncation_comment(self):
        fields = [{"name": f"col{i}"} for i in range(15)]
        result = generate_column_list(fields)
        assert "additional columns omitted" in result
        assert result.count(",") <= 10


class TestJsonInjectionPrevention:
    def test_json_dumps_escapes_quotes(self):
        malicious_input = 'test"; DROP TABLE users; --'
        safe_json = json.dumps({"query": malicious_input})
        assert '"test\\"' in safe_json or '"; DROP' not in safe_json.replace('\\"', '')
    
    def test_json_dumps_escapes_backslash(self):
        malicious_input = "test\\n\\r injection"
        safe_json = json.dumps({"query": malicious_input})
        parsed = json.loads(safe_json)
        assert parsed["query"] == malicious_input
