import datetime


class ErrorLogger:
    def __init__(self, filename):
        """
        Конструктор класса ErrorLogger.

        :param filename: Имя файла, в который будут записываться ошибки.
        :type filename: str
        """
        self.filename = filename
        self.log_file = None

    def __enter__(self):
        """
        Метод, вызываемый при входе в контекстный менеджер.

        Открывает файл для записи ошибок и возвращает экземпляр ErrorLogger.

        :return: Экземпляр ErrorLogger.
        """
        self.log_file = open(self.filename, 'a')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Метод, вызываемый при выходе из контекстного менеджера.

        При наличии ошибки (exc_type != None), записывает информацию об ошибке
        в файл, включая тип ошибки и метку времени. Затем закрывает файл.

        :param exc_type: Тип ошибки.
        :type exc_type: type
        :param exc_value: Сообщение об ошибке.
        :type exc_value: Exception
        :param traceback: Информация о стеке вызовов.
        :type traceback: traceback object
        :return: Возвращает False, чтобы продолжить вызов ошибки.
        """
        if exc_type is not None:
            time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            message = f"Error at {time}: {exc_type.__name__}: {exc_value}\n"
            self.log_file.write(message)
        self.log_file.close()
        return False


with ErrorLogger('error_log.txt'):
    result = 1 / 0  # ZeroDivisionError: division by zero
