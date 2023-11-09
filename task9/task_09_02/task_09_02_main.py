import os
import json
from copy import deepcopy

from task_09_02_peewee_model import (EmailsTable, SalaryTable, ReportTable,
                                     EmployeesTable, MySqliteDatabase)


# Функция для создания таблиц (Create)
def create_tables(tables: list[MySqliteDatabase]):
    """
    Создает таблицы в базе данных.

    :param tables: Список моделей таблиц.
    """
    for table in tables:
        table.create_table()


# Функция для извлечения данных из таблицы (Exctract)
def extract(Table: MySqliteDatabase):
    """
    Извлекает данные из таблицы и возвращает их в виде списка списков.

    :param Table: Модель таблицы, из которой нужно извлечь данные.
    :return: Список списков с данными из таблицы.
    """
    fields = [field for field in Table._meta.fields]

    if Table == ReportTable:
        fields = fields[1:] + [fields[0]]

    datas = [
        [getattr(data, field) for field in fields] for data in Table.select()
        ]

    return datas


# Функция для преобразования данных (Transform)
def transform(Table: MySqliteDatabase, data: list[list], report: dict[dict]):
    """
    Преобразует данные и возвращает отчет.

    :param Table: Модель таблицы для преобразования данных.
    :param data: Данные для преобразования.
    :param report: Исходный отчет, который обновляется.
    :return: Обновленный отчет.
    """
    report = deepcopy(report)

    if Table == EmployeesTable:
        for row in data:
            maket = {
                'fio': f'{row[1]} {row[2]} {row[3]}',
                'salary': 0,
                'bonus': 0,
                'cnt_bonus': 0,
                'cnt_salary': 0,
                'emails': [None]
            }
            report[row[0]] = maket

    if Table == SalaryTable:
        for row in data:
            report[row[0]][row[2]] += row[3]
            report[row[0]][f'cnt_{row[2]}'] += 1

    if Table == EmailsTable:
        for row in data:
            email = report[row[1]]['emails']
            report[row[1]]['emails'] = [row[2]] if email == [None] else email + [row[2]]

        final_report = []
        for key, val in report.items():
            Empl_ID = key
            fio = val['fio']
            Salary = 0 if not val['cnt_salary'] else val['salary'] / val['cnt_salary']
            Bonus = 0 if not val['cnt_bonus'] else val['bonus'] / val['cnt_bonus']

            for email in set(val['emails']):
                final_report += [[email, Empl_ID, fio, Salary, Bonus]]
        return final_report
    return report


# Функция для загрузки данных в таблицу (Load)
def load_data_into_table(Table: MySqliteDatabase, data: list[list]):
    """
    Загружает данные в таблицу.

    :param Table: Модель таблицы, в которую нужно загрузить данные.
    :param data: Данные для загрузки.
    """
    fields = Table._meta.fields

    for row in data:
        field_data = {field: value for field, value in zip(fields, row)}
        Table.create(**field_data)


# Функция для удаления таблиц (Delete)
def delete_tables(tables: list[MySqliteDatabase]):
    """
    Удаляет таблицы из базы данных.

    :param tables: Список моделей таблиц для удаления.
    """
    for table in tables:
        table.drop_table()


if __name__ == '__main__':
    # Получение путей к БД и к JSON с начальными данными
    current_directory_path = os.path.dirname(os.path.abspath(__file__))
    path_to_json_data = f"{current_directory_path}\\data.json"

    # Создание таблиц
    tables = [EmployeesTable, SalaryTable, EmailsTable, ReportTable]
    create_tables(tables)

    # Чтение данных из файла JSON
    basic_data = ''
    with open(path_to_json_data, "r", encoding="utf-8") as json_file:
        basic_data = json.load(json_file)

    # Заполнение таблиц начальными данными
    load_data_into_table(EmployeesTable, basic_data['employees'])
    load_data_into_table(SalaryTable, basic_data['salary'])
    load_data_into_table(EmailsTable, basic_data['emails'])

    # Получение данных из таблиц
    employees_data = extract(EmployeesTable)
    salary_data = extract(SalaryTable)
    emails_data = extract(EmailsTable)

    # Преобразование данных из таблиц
    report = {}
    report = transform(EmployeesTable, employees_data, report)
    report = transform(SalaryTable, salary_data, report)
    final_report = transform(EmailsTable, emails_data, report)

    # Загрузка финального отчета 'final_report' в твблицу 'ReportTable'
    load_data_into_table(ReportTable, final_report)

    # Проверка загруженных данных
    report_data = extract(ReportTable)
    print(*[' '.join(list(map(str, data))) for data in report_data], sep='\n')

    # Удаление таблиц
    delete_tables(tables)
