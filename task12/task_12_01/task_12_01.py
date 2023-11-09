%pyspark
%matplotlib inline

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import seaborn as sns
import matplotlib.pyplot as plt


spark = SparkSession.builder.appName("OlympicsAnalysis").getOrCreate()

df = spark.read.option("header", "true").csv("/data/data/olympics.csv")
df.createOrReplaceTempView("olympics_data")

# На какой дистанции Jesse Owens выиграл медаль?
cn = "OWENS, Jesse"
jesse_owens_medal_event = df.filter(col("Athlete") == cn).select("Event").first()["Event"]
print(f"Jesse Owens выиграл медаль в событии: {jesse_owens_medal_event}")

# Мужчины какой страны взяли большинство золотых медалей в бадминтоне?
sport = "Badminton"
badminton_gold_medals = df.filter((col("Sport") == sport) & (col("Medal") == "Gold"))
most_gold_medals_country = (badminton_gold_medals
                            .filter(col("Gender") == "Men")
                            .groupBy("NOC").count()
                            .orderBy("count", ascending=False)
                            .first()["NOC"])
print(f"Мужчины из страны {most_gold_medals_country}")

# Какие три страны получили наибольшее количество медалей в последние
# годы (с 1984 по 2008)?
print("Топ-3 страны с наибольшим количеством медалей в последние годы:")
recent_medals = df.filter((col("Edition").between(1984, 2008)))
top_countries = (recent_medals
                 .groupBy("NOC").count()
                 .orderBy("count", ascending=False)
                 .limit(3)
                 .select("NOC", "count")
                 .show())

# Найдите мужчин - золотых медалистов по 100m. Выведите город, год, имя атлета
# и страну, за которую он выступал.
print("Мужчины-золотые медалисты по 100m:")
men_100m_gold_medalists = (df.filter((col("Event") == "100m") & (col("Medal") == "Gold") & (col("Gender") == "Men"))
                           .select("City", "Edition", "Athlete", "NOC")
                           .orderBy("Edition", ascending=False)
                           .show())

# Число всех медалей, выигранных на каждой Олимпиаде
medals_by_year_sql = spark.sql('''
    SELECT Edition, COUNT(*) AS count
    FROM olympics_data
    GROUP BY Edition
    ORDER BY Edition
''')

medals_by_year_pd = medals_by_year_sql.toPandas()
medals_by_year_pd["count"] = medals_by_year_pd["count"].astype(int)

plt.figure(figsize=(10, 5))
sns.barplot(x="Edition", y="count", data=medals_by_year_pd)
plt.xlabel("Год Олимпиады")
plt.ylabel("Количество медалей")
plt.title("Число всех медалей по годам")
plt.show()

# Статистика медалей по странам
medals_by_country = spark.sql('''
    SELECT NOC, COUNT(*) AS `Total Medals`, MIN(Edition) AS `First Year`, MAX(Edition) AS `Last Year`
    FROM olympics_data
    WHERE Medal IS NOT NULL
    GROUP BY NOC
    ORDER BY `Total Medals` DESC
''')
print("Статистика медалей по странам:")
medals_by_country.show()

# Золотые медали по полу в атлетике
usa_athletics = spark.sql('''
    SELECT Gender, COUNT(*) AS count
    FROM olympics_data
    WHERE NOC = 'USA' AND Sport = 'Athletics' AND Medal = 'Gold'
    GROUP BY Gender
''').toPandas()

sns.barplot(x="Gender", y="count", data=usa_athletics)
plt.xlabel("Пол")
plt.ylabel("Количество золотых медалей")
plt.title("Золотые медали в атлетике для мужчин и женщин из США")
plt.show()

# Топ-атлеты США по годам
usa_athletes_by_year = spark.sql('''
    SELECT Edition, FIRST(Athlete) AS Athlete, FIRST(Discipline) AS Discipline, MAX(count) AS `Total Medals`
    FROM (
        SELECT Edition, Athlete, Discipline, COUNT(*) AS count
        FROM olympics_data
        WHERE NOC = 'USA'
        GROUP BY Edition, Athlete, Discipline
    )
    GROUP BY Edition
    ORDER BY Edition
''')
print("Топ-атлеты США по годам:")
usa_athletes_by_year.show()
