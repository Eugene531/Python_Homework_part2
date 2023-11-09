from peewee import (DateField, SqliteDatabase, Model,
                    IntegerField, CharField, DoubleField)


# Имя БД
db_name = 'my_database.db'

# Создание объекта БД
db = SqliteDatabase(db_name)


# Определение базовой модели данных для всех таблиц
class MySqliteDatabase(Model):
    class Meta:
        database = db  # модель будет использовать БД 'my_database.db'


# Определение модели данных для таблицы "EmployeesTable"
class EmployeesTable(MySqliteDatabase):
    id = IntegerField()  # ID сотрудника
    name1 = CharField()  # Фамилия сотрудника
    name2 = CharField()  # Имя сотрудника
    name3 = CharField()  # Отчество сотрудника


# Определение модели данных для таблицы "SalaryTable"
class SalaryTable(MySqliteDatabase):
    id = IntegerField()        # ID сотрудника
    dt = DateField()           # Дата выплаты
    salary_type = CharField()  # Тип суммы (salary, bonus)
    amount = DoubleField()     # Выплаченная сумма


# Определение модели данных для таблицы "EmailsTable"
class EmailsTable(MySqliteDatabase):
    id = IntegerField()        # ID Email
    empl_id = IntegerField()   # ID сотрудника
    email = CharField()        # Email адрес


# Определение модели данных для таблицы "ReportTable"
class ReportTable(MySqliteDatabase):
    email = CharField(primary_key=True, null=True)
    empl_id = IntegerField()   # ID сотрудника
    fio = CharField()          # ФИО сотрудника
    salary = DoubleField()     # Средняя ЗП сотрудника
    bonus = DoubleField()      # Средний бонус сотрудника
