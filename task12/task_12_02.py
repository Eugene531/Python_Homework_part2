%pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, avg


spark = SparkSession.builder.appName("Чтение CSV").getOrCreate()

employees = [
    (1, 'Шершуков', 'Виктор', 'Кузьмич',),
    (2, 'Битова', 'Анастасия', 'Юрьевна',),
    (3, 'Кириллов', 'Валентин', 'Владиславович',),
    (4, 'Игнатьев', 'Игорь', 'Дмитриевич',),
]

emails = [
    (1, 1, 'shershuko@mail.ru',),
    (2, 1, 'shershuko-v@mail.ru',),
    (3, 2, 'bitova@mail.ru',),
    (4, 2, 'bitova@mail.ru',),
    (5, 3, 'kirillov@mail.ru',),
    (6, 3, 'kirillov@mail.ru',),
]

salary = [
    (1, '2019-12-01', 'salary', 50000),
    (1, '2020-01-01', 'salary', 50000),
    (1, '2020-02-01', 'salary', 50000),
    (1, '2020-03-01', 'salary', 50000),
    (1, '2020-04-01', 'salary', 50000),
    (1, '2020-05-01', 'salary', 50000),
    (1, '2020-06-01', 'salary', 53000),
    (1, '2020-07-01', 'salary', 53000),
    (1, '2020-08-01', 'salary', 53000),
    (1, '2020-09-01', 'salary', 53000),
    (1, '2020-10-01', 'salary', 53000),
    (1, '2020-11-01', 'salary', 53000),
    (1, '2020-12-01', 'salary', 53000),
    (1, '2021-01-01', 'salary', 53000),
    (1, '2019-12-01', 'bonus', 10000),
    (1, '2020-01-01', 'bonus', 10000),
    (1, '2020-02-01', 'bonus', 9000),
    (1, '2020-03-01', 'bonus', 11000),
    (1, '2020-04-01', 'bonus', 10000),
    (1, '2020-05-01', 'bonus', 5000),
    (1, '2020-06-01', 'bonus', 10000),
    (1, '2020-07-01', 'bonus', 10000),
    (1, '2020-08-01', 'bonus', 10000),
    (1, '2020-09-01', 'bonus', 10000),
    (1, '2020-10-01', 'bonus', 10000),
    (1, '2020-11-01', 'bonus', 10000),
    (1, '2020-12-01', 'bonus', 10000),
    (1, '2021-01-01', 'bonus', 10000),
    (2, '2019-12-01', 'salary', 60000),
    (2, '2020-01-01', 'salary', 60000),
    (2, '2020-02-01', 'salary', 60000),
    (2, '2020-03-01', 'salary', 62000),
    (2, '2020-04-01', 'salary', 62000),
    (2, '2020-05-01', 'salary', 62000),
    (2, '2020-06-01', 'salary', 62000),
    (2, '2020-07-01', 'salary', 62000),
    (2, '2020-08-01', 'salary', 62000),
    (2, '2020-09-01', 'salary', 62000),
    (2, '2020-10-01', 'salary', 65000),
    (2, '2020-11-01', 'salary', 65000),
    (2, '2020-12-01', 'salary', 65000),
    (2, '2021-01-01', 'salary', 65000),
    (2, '2019-12-01', 'bonus', 10000),
    (2, '2020-01-01', 'bonus', 10000),
    (2, '2020-02-01', 'bonus', 9000),
    (2, '2020-03-01', 'bonus', 11000),
    (2, '2020-04-01', 'bonus', 10000),
    (2, '2020-05-01', 'bonus', 5000),
    (2, '2020-06-01', 'bonus', 10000),
    (2, '2020-07-01', 'bonus', 7000),
    (2, '2020-08-01', 'bonus', 7000),
    (2, '2020-09-01', 'bonus', 7000),
    (2, '2020-10-01', 'bonus', 7000),
    (2, '2020-11-01', 'bonus', 7000),
    (2, '2020-12-01', 'bonus', 7000),
    (2, '2021-01-01', 'bonus', 7000),
    (3, '2019-12-01', 'salary', 60000),
    (3, '2020-01-01', 'salary', 60000),
    (3, '2020-02-01', 'salary', 60000),
    (3, '2020-03-01', 'salary', 60000),
    (3, '2020-04-01', 'salary', 60000),
    (3, '2020-05-01', 'salary', 60000),
    (3, '2020-06-01', 'salary', 60000),
    (3, '2020-07-01', 'salary', 60000),
    (3, '2020-08-01', 'salary', 60000),
    (3, '2020-09-01', 'salary', 60000),
    (3, '2020-10-01', 'salary', 60000),
    (3, '2020-11-01', 'salary', 64000),
    (3, '2020-12-01', 'salary', 64000),
    (3, '2021-01-01', 'salary', 64000),
    (4, '2019-12-01', 'salary', 61000),
    (4, '2020-01-01', 'salary', 61000),
    (4, '2020-02-01', 'salary', 61000),
    (4, '2020-03-01', 'salary', 61000),
    (4, '2020-04-01', 'salary', 61000),
    (4, '2020-05-01', 'salary', 63000),
    (4, '2020-06-01', 'salary', 63000),
    (4, '2020-07-01', 'salary', 63000),
    (4, '2020-08-01', 'salary', 63000),
    (4, '2020-09-01', 'salary', 63000),
    (4, '2020-10-01', 'salary', 63000),
    (4, '2020-11-01', 'salary', 63000),
    (4, '2020-12-01', 'salary', 63000),
    (4, '2021-01-01', 'salary', 63000),
    (4, '2019-12-01', 'bonus', 7000),
    (4, '2020-01-01', 'bonus', 7000),
    (4, '2020-02-01', 'bonus', 7000),
    (4, '2020-03-01', 'bonus', 7000),
    (4, '2020-04-01', 'bonus', 7000),
    (4, '2020-05-01', 'bonus', 7000),
    (4, '2020-06-01', 'bonus', 7000),
    (4, '2020-07-01', 'bonus', 7000),
    (4, '2020-08-01', 'bonus', 7000),
    (4, '2020-09-01', 'bonus', 7000),
    (4, '2020-10-01', 'bonus', 7000),
    (4, '2020-11-01', 'bonus', 7000),
    (4, '2020-12-01', 'bonus', 7000),
    (4, '2021-01-01', 'bonus', 7000),
]

#############################################################
#                      DataFrame API                        #
#############################################################

employees_df = spark.createDataFrame(employees, ('Empl_ID', 'n1', 'n2', 'n3'))
emails_df = spark.createDataFrame(emails, ('ID', 'Empl_ID', 'Email'))
salary_df = spark.createDataFrame(salary, ('Empl_ID', 'dt', 'Type', 'Amount'))

# Объединение таблиц 'employees_df' и 'salary_df'
merged_df = employees_df.join(salary_df, on='Empl_ID', how='left')

# Вычисление средних значений
pivot_table = (merged_df.groupBy('Empl_ID', 'n1', 'n2', 'n3')
               .pivot('Type')
               .agg(avg('Amount').alias('Amount'))
               .fillna(0))

final_df = pivot_table.join(emails_df, on='Empl_ID', how='outer')

final_df = final_df.dropDuplicates(['Empl_ID', 'Email'])

# Создание поля 'FIO'
final_df = final_df.withColumn(
    'FIO', concat(col('n1'), lit(' '), col('n2'), lit(' '), col('n3')))

final_df = final_df.drop('n1', 'n2', 'n3', 'ID')

final_df.show()

#############################################################
#                        Spark SQL                          #
#############################################################


# Регистрируем DataFrame как временное представление
employees_df.createOrReplaceTempView("employees_temp")
emails_df.createOrReplaceTempView("emails_temp")
salary_df.createOrReplaceTempView("salary_temp")

result_df = spark.sql('''
    SELECT DISTINCT
        p.Empl_ID,
        p.FIO,
        p.Bonus,
        p.Salary,
        e.Email
    FROM (
        SELECT
            e.Empl_ID,
            concat(e.n1, e.n2, e.n3) as FIO,
            COALESCE(AVG(CASE WHEN s.TYPE = 'bonus' THEN s.AMOUNT END), 0) as Bonus,
            COALESCE(AVG(CASE WHEN s.TYPE = 'salary' THEN s.AMOUNT END), 0) as Salary
        FROM employees_temp e
        LEFT JOIN salary_temp s ON e.Empl_ID = s.Empl_ID
        GROUP BY e.Empl_ID, FIO
    ) as p
    LEFT JOIN emails_temp e ON p.Empl_ID = e.Empl_ID
''')

result_df.show()
