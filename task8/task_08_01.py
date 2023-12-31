def myzip(*args):
    """
    Реализует схожий функционал с zip из стандартной библиотеки Python.

    :param args: Передаваемые аргументы - итерируемые объекты.
    :return: Генератор.

    Пример:
    >>> list(myzip(['A', 'B', 'C'], [1, 2, 3]))
    ['A', 1, 'B', 2, 'C', 3]

    >>> list(myzip('!', ['A', 'B', 'C', 'D'], range(1, 3)))
    ['!', 'A', 1, 'B', 2, 'C', 'D']

    Функция выполняет итерации по переданным объектам и создает кортежи,
    содержащие элементы на текущих позициях. Если длины переданных объектов
    различны, функция завершит выполнение, когда достигнет конца самого
    длинного объекта.
    """
    for i in range(len(max(args, key=len))):
        for iter_obj in args:
            if i + 1 <= len(iter_obj):
                yield iter_obj[i]


res1 = list(myzip(['A', 'B', 'C'], [1, 2, 3]))
print(res1)
# ['A', 1, 'B', 2, 'C', 3]
res2 = list(myzip('!', ['A', 'B', 'C', 'D'], range(1, 3)))
print(res2)
# ['!', 'A', 1, 'B', 2, 'C', 'D']
