import os
import pickle

from dotenv import load_dotenv
from neo4j import GraphDatabase

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

load_dotenv()

# pull env vars for auth and create neo4j driver
NEO4J_AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
NEO4J_URI = os.getenv("NEO4J_URI")
# NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

try:
    df3 = pd.read_pickle("./cache.pkl")

except FileNotFoundError:
    with GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH) as driver:
        with driver.session() as session:
            three_year = session.run(
                """
MATCH (countries:Country)
UNWIND countries AS country
WITH range(1,155) AS weekNumberList, country
UNWIND weekNumberList AS weekNumber
WITH date("2019-01-01") + duration({weeks: weekNumber}) AS startDate, 20 AS baselineYears, country
WITH startDate, range(1, baselineYears) as baselineRange, country
MATCH (country)<-[:IN]-(report:Report)
UNWIND baselineRange AS yearDiff
WITH country, report, startDate, (startDate - duration({years: yearDiff})) AS date 
WHERE date - duration({days: 15}) < report.start < date + duration({days: 15})
WITH startDate, avg(report.positive) AS baseline, stDev(report.positive) AS dev, country
MATCH (country)<-[:IN]-(dayReport:Report)
WHERE dayReport.start < startDate < dayReport.start + dayReport.duration
WITH startDate, baseline, (dayReport.positive - baseline)/dev AS `standard deviations from 10 year baseline`, country
RETURN country.name as country, startDate, avg(`standard deviations from 10 year baseline`) as devs
            """
            )

            print(three_year.peek())

            df3 = pd.DataFrame(three_year, columns=three_year.keys())
            df3["startDate"] = df3["startDate"].apply(lambda x: x.to_native())
            print(df3)

            pd.to_pickle(df3, "./cache.pkl")


print(df3)
df3 = df3.pivot(
    index="startDate",
    columns="country",
    values="devs",
)
df3 = df3.T
# df3 = df3[df3.isnull().sum(axis=1) < 50]
df3 = df3.dropna()
print(df3)
df3 = df3.T
df3 = df3.drop("Australia", axis=1)
# df3 = df3.drop("Canada", axis=1)
# df3 = df3.drop("Oman", axis=1)
df3 = df3.drop("South Africa", axis=1)

print(df3)

df4 = pd.DataFrame()
for column in df3.columns:
    df4[column] = df3[column].rolling(window=4, center=True).mean()

df4 = df4.reset_index().rename(columns={df4.index.name: "startDate"})
df4 = df4.melt(id_vars=["startDate"], var_name="country", value_name="devs")

# id_vars=["startDate"]

print(df4)

fig, ax = plt.subplots()

custom_params = {
    "axes.spines.right": False,
    "axes.spines.top": False,
    "axes.spines.bottom": False,
    # "axes.labels.bottom": False,
}
sns.set_theme(style="ticks", rc=custom_params)

g = sns.FacetGrid(df4, col="country", col_wrap=4)
g.map_dataframe(sns.lineplot, x="startDate", y="devs")
g.add_legend()
g.refline(y=0)

plt.show()
