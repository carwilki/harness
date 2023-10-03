import pytest
from harness.utils.schema import stripPrefix


def test_stripPrefix():
    assert stripPrefix("qa_schema") == "schema"
    assert stripPrefix("dev_schema") == "schema"
    assert stripPrefix("qa_schema_test") == "schema_test"
    assert stripPrefix("dev_schema_test") == "schema_test"
    assert stripPrefix("qa_schema_test_123") == "schema_test_123"
    assert stripPrefix("dev_schema_test_123") == "schema_test_123"
    
    with pytest.raises(ValueError):
        stripPrefix("prod_schema")
    with pytest.raises(ValueError):
        stripPrefix("qa")
    with pytest.raises(ValueError):
        stripPrefix("dev")
    with pytest.raises(ValueError):
        stripPrefix("prod")