import unittest

from modules_to_test.task_06_02 import Euro, Dollar, Ruble


class CurrencyTests(unittest.TestCase):
    def test_create_currency_instance(self):
        """
        Проверка создания экземпляров валютных классов.
        """
        euro = Euro(5)
        dollar = Dollar(10)
        ruble = Ruble(100)

        self.assertIsInstance(euro, Euro)
        self.assertIsInstance(dollar, Dollar)
        self.assertIsInstance(ruble, Ruble)
        self.assertEqual(euro.amount, 5)
        self.assertEqual(dollar.amount, 10)
        self.assertEqual(ruble.amount, 100)

    def test_currency_str_representation(self):
        """
        Проверка строкового представления валюты.
        """
        euro = Euro(5)
        dollar = Dollar(10)
        ruble = Ruble(100)

        self.assertEqual(str(euro), "5 €")
        self.assertEqual(str(dollar), "10 $")
        self.assertEqual(str(ruble), "100 ")

    def test_add_currency(self):
        """
        Проверка операции сложения валют.
        """
        euro1 = Euro(5)
        euro2 = Euro(3)
        dollar1 = Dollar(10)
        dollar2 = Dollar(5)
        ruble1 = Ruble(100)
        ruble2 = Ruble(50)

        result1 = euro1 + euro2
        result2 = dollar1 + dollar2
        result3 = ruble1 + ruble2

        self.assertIsInstance(result1, Euro)
        self.assertIsInstance(result2, Dollar)
        self.assertIsInstance(result3, Ruble)
        self.assertEqual(result1.amount, 8)
        self.assertEqual(result2.amount, 15)
        self.assertEqual(result3.amount, 150)

    def test_subtract_currency(self):
        """
        Проверка операции вычитания валют.
        """
        euro1 = Euro(5)
        euro2 = Euro(3)
        dollar1 = Dollar(10)
        dollar2 = Dollar(5)
        ruble1 = Ruble(100)
        ruble2 = Ruble(50)

        result1 = euro1 - euro2
        result2 = dollar1 - dollar2
        result3 = ruble1 - ruble2

        self.assertIsInstance(result1, Euro)
        self.assertIsInstance(result2, Dollar)
        self.assertIsInstance(result3, Ruble)
        self.assertEqual(result1.amount, 2)
        self.assertEqual(result2.amount, 5)
        self.assertEqual(result3.amount, 50)

    def test_multiply_currency_by_scalar(self):
        """
        Проверка операции умножения валюты на скаляр.
        """
        euro = Euro(5)
        dollar = Dollar(10)
        ruble = Ruble(100)
        scalar = 2

        result1 = euro * scalar
        result2 = dollar * scalar
        result3 = ruble * scalar

        self.assertIsInstance(result1, Euro)
        self.assertIsInstance(result2, Dollar)
        self.assertIsInstance(result3, Ruble)
        self.assertEqual(result1.amount, 10)
        self.assertEqual(result2.amount, 20)
        self.assertEqual(result3.amount, 200)

    def test_divide_currency_by_scalar(self):
        """
        Проверка операции деления валюты на скаляр.
        """
        euro = Euro(10)
        dollar = Dollar(20)
        ruble = Ruble(200)
        scalar = 2

        result1 = euro / scalar
        result2 = dollar / scalar
        result3 = ruble / scalar

        self.assertIsInstance(result1, Euro)
        self.assertIsInstance(result2, Dollar)
        self.assertIsInstance(result3, Ruble)
        self.assertEqual(result1.amount, 5)
        self.assertEqual(result2.amount, 10)
        self.assertEqual(result3.amount, 100)

    def test_equal_currency(self):
        """
        Проверка операции сравнения валют на равенство.
        """
        euro1 = Euro(5)
        euro2 = Euro(5)
        dollar1 = Dollar(10)
        dollar2 = Dollar(20)
        ruble1 = Ruble(100)
        ruble2 = Ruble(100)

        self.assertTrue(euro1 == euro2)
        self.assertTrue(ruble1 == ruble2)
        self.assertFalse(euro1 == dollar1)
        self.assertFalse(dollar1 == dollar2)
        self.assertFalse(dollar1 == ruble1)

    def test_compare_currency(self):
        """
        Проверка операции сравнения валют.
        """
        euro1 = Euro(5)
        euro2 = Euro(10)
        dollar1 = Dollar(10)
        dollar2 = Dollar(5)
        ruble1 = Ruble(100)
        ruble2 = Ruble(200)

        self.assertTrue(euro1 < euro2)
        self.assertFalse(euro1 > euro2)
        self.assertFalse(dollar1 < dollar2)
        self.assertTrue(dollar1 > dollar2)
        self.assertTrue(ruble1 < ruble2)
        self.assertFalse(ruble1 > ruble2)

    def test_currency_conversion(self):
        """
        Проверка операции конвертации валюты.
        """
        euro = Euro(5)
        dollar = Dollar(10)
        ruble = Ruble(100)

        dollar_result = euro.to(Dollar)
        euro_result = dollar.to(Euro)
        ruble_result = ruble.to(Ruble)

        self.assertIsInstance(dollar_result, Dollar)
        self.assertIsInstance(euro_result, Euro)
        self.assertIsInstance(ruble_result, Ruble)
        self.assertEqual(dollar_result.amount, 6)
        self.assertEqual(euro_result.amount, 8.333333333333334)
        self.assertEqual(ruble_result.amount, 100.0)


if __name__ == '__main':
    unittest.main()
