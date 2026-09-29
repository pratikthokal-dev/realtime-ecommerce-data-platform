import unittest


VALID_OPERATIONS = {"c", "u", "d", "r"}


def validate_operation(operation):
    return operation in VALID_OPERATIONS


def validate_amount(operation, amount):
    if operation == "d":
        return True

    return amount is not None and amount >= 0


class TestCDCPipeline(unittest.TestCase):

    def test_valid_cdc_operations(self):
        for operation in ["c", "u", "d", "r"]:
            self.assertTrue(validate_operation(operation))

    def test_invalid_cdc_operation(self):
        self.assertFalse(validate_operation("x"))

    def test_valid_amount(self):
        self.assertTrue(validate_amount("c", 2500.00))
        self.assertTrue(validate_amount("u", 10000.00))

    def test_delete_can_have_null_amount(self):
        self.assertTrue(validate_amount("d", None))

    def test_negative_amount_is_invalid(self):
        self.assertFalse(validate_amount("c", -100))


if __name__ == "__main__":
    unittest.main()