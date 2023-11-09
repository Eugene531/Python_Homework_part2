import pandas as pd
import matplotlib.pyplot as plt


# Загрузка данных из файла olympics.csv
df = pd.read_csv('olympics.csv', skiprows=4)

# На какой дистанции Jesse Owens выиграл медаль?
cn = "OWENS, Jesse"
jesse_owens_medal_event = df[df["Athlete"] == cn]["Event"].values[0]
print(f"Jesse Owens выиграл медаль в событии: {jesse_owens_medal_event}")

# Мужчины какой страны взяли большинство золотых медалей в бадминтоне?
sport = "Badminton"
badminton_gold_medals = df[(df["Sport"] == sport) & (df["Medal"] == "Gold")]
most_gold_medals_country = badminton_gold_medals[
    badminton_gold_medals["Gender"] == "Men"]["NOC"].mode()[0]
print(f"Мужчины из страны {most_gold_medals_country}")

# Какие три страны получили наибольшее количество медалей в последние
# годы (с 1984 по 2008)?
recent_years = range(1984, 2009)
recent_medals = df[df["Edition"].isin(recent_years)]
top_countries = recent_medals["NOC"].value_counts().head(3)
print("Топ-3 страны с наибольшим количеством медалей в последние годы:")
print(top_countries)

# Найдите мужчин - золотых медалистов по 100m. Выведите город, год, имя атлета
# и страну, за которую он выступал.
men_100m_gold_medalists = df[
    (df["Event"] == "100m") & (df["Medal"] == "Gold") & (df["Gender"] == "Men")
    ]
result = men_100m_gold_medalists[
    ["City", "Edition", "Athlete", "NOC"]
    ].sort_values(by="Edition", ascending=False)
print("Мужчины-золотые медалисты по 100m:")
print(result)

# Используя groupby(), постройте график - число всех медалей, выигранных на
# каждой Олимпиаде.
medals_by_year = df.groupby("Edition")["Medal"].count()
medals_by_year.plot(kind="bar", figsize=(10, 5))
plt.xlabel("Год Олимпиады")
plt.ylabel("Количество медалей")
plt.title("Число всех медалей по годам")
plt.show()

# Создайте список, показывающий число всех медалей, полученных каждой страной в
# течение всей истории Олимпийских игр.
# Для каждой страны покажите год первой и последней заработанной медали.
medals_by_country = df.groupby("NOC").agg(
    {"Medal": "count", "Edition": ["min", "max"]})
medals_by_country.columns = ["Total Medals", "First Year", "Last Year"]
medals_by_country = medals_by_country.reset_index()
print("Статистика медалей по странам:")
print(medals_by_country)

# Постройте график - число золотых медалей, выигранных мужчинами и женщинами из
# США в атлетике.
usa_athletics = df[(df["NOC"] == "USA") & (df["Sport"] == "Athletics")]
usa_gold_medals_by_gender = usa_athletics.groupby("Gender")["Medal"].count()
usa_gold_medals_by_gender.plot(kind="bar", figsize=(8, 5))
plt.xlabel("Пол")
plt.ylabel("Количество золотых медалей")
plt.title("Золотые медали в атлетике для мужчин и женщин из США")
plt.show()

# Постройте таблицу, где по годам всех Олимпиад покажите атлетов США
# (один атлет на год), топовых по общему количеству медалей.
# Также укажите дисциплину, в которой выступал атлет.
usa_athletes_by_year = df[df["NOC"] == "USA"].groupby(
    ["Edition", "Athlete", "Discipline"])["Medal"].count().reset_index()
usa_top_athletes_by_year = usa_athletes_by_year.groupby("Edition").apply(
    lambda x: x[x["Medal"] == x["Medal"].max()])
print("Топ-атлеты США по годам:")
print(usa_top_athletes_by_year)
