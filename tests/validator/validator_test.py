from harness.validator.AbstractValidator import AbstractValidator


class TestValidator:
    def test_config(self):
        validator = AbstractValidator()
        assert validator is not None
