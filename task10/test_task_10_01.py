import unittest

from modules_to_test.task_06_01 import Matrix


class MatrixMethodsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        """Установка тестовых матриц перед выполнением тестов."""
        cls.square_marix = Matrix(
            [
                [1, 2, 3],
                [4, 5, 6],
                [7, 8, 9]
            ]
        )

        cls.symmetric_marix = Matrix(
            [
                [1, 2, 3],
                [2, 1, 4],
                [3, 4, 1]
            ]
        )

    def test_add_matrixs(self):
        """Тест для сложения матриц."""
        test_value = self.square_marix + self.symmetric_marix
        expected_value = Matrix([[2, 4, 6], [6, 6, 10], [10, 12, 10]])
        self.assertEqual(test_value, expected_value)

    def test_sub_matrixs(self):
        """Тест для вычитания матриц."""
        test_value1 = self.square_marix - self.symmetric_marix
        test_value2 = self.symmetric_marix - self.square_marix
        expected_value1 = Matrix([[0, 0, 0], [2, 4, 2], [4, 4, 8]])
        expected_value2 = Matrix([[0, 0, 0], [-2, -4, -2], [-4, -4, -8]])
        self.assertEqual(test_value1, expected_value1)
        self.assertEqual(test_value2, expected_value2)

    def test_mul_matrixs(self):
        """Тест для умножения матриц."""
        test_value1 = self.square_marix * self.symmetric_marix
        test_value2 = self.symmetric_marix * self.square_marix
        expected_value1 = Matrix([[14, 16, 14], [32, 37, 38], [50, 58, 62]])
        expected_value2 = Matrix([[30, 36, 42], [34, 41, 48], [26, 34, 42]])
        self.assertEqual(test_value1, expected_value1)
        self.assertEqual(test_value2, expected_value2)

    def test_mul_matrix_and_scalar(self):
        """Тест для умножения матрицы на скаляр."""
        test_value1 = self.square_marix * 32
        test_value2 = self.symmetric_marix * 123
        expected_value1 = Matrix(
            [[32, 64, 96], [128, 160, 192], [224, 256, 288]])
        expected_value2 = Matrix(
            [[123, 246, 369], [246, 123, 492], [369, 492, 123]])
        self.assertEqual(test_value1, expected_value1)
        self.assertEqual(test_value2, expected_value2)

    def test_transpose_matrixs(self):
        """Тест для метода транспонирования матриц."""
        test_value1 = self.square_marix.transpose()
        test_value2 = self.symmetric_marix.transpose()
        expected_value1 = Matrix([[1, 4, 7], [2, 5, 8], [3, 6, 9]])
        expected_value2 = Matrix([[1, 2, 3], [2, 1, 4], [3, 4, 1]])
        self.assertEqual(test_value1, expected_value1)
        self.assertEqual(test_value2, expected_value2)

    def test_eq_matrixs(self):
        """Тест для проверки равенства матриц."""
        self.assertTrue(self.square_marix == self.square_marix)
        self.assertTrue(self.symmetric_marix == self.symmetric_marix)
        self.assertFalse(self.square_marix == self.symmetric_marix)
        self.assertFalse(self.symmetric_marix == self.square_marix)

    def test_is_square_matrix(self):
        """Тест для метода проверки, является ли матрица квадратной."""
        not_square_matrix = Matrix([[1, 2]])
        self.assertTrue(self.square_marix.is_square())
        self.assertTrue(self.symmetric_marix.is_square())
        self.assertFalse(not_square_matrix.is_square())

    def test_is_symmetric_matrix(self):
        """Тест для метода проверки, является ли матрица симметричной."""
        self.assertFalse(self.square_marix.is_symmetric())
        self.assertTrue(self.symmetric_marix.is_symmetric())


if __name__ == '__main':
    unittest.main()
