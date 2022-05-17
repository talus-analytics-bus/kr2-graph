import os

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
NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)


with NEO4J_DRIVER.session() as session:
    three_year = session.run(
        """
MATCH (countries:Country)
UNWIND countries AS country
WITH range(1,155) AS weekNumberList, country
UNWIND weekNumberList AS weekNumber
WITH date("2017-01-01") + duration({weeks: weekNumber}) AS startDate, 20 AS baselineYears, country
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

    # three_year = session.run(
    #     """
    #     WITH range(0,104) AS weekNumberList UNWIND weekNumberList AS weekNumber
    #     WITH date("2020-01-01") + duration({weeks: weekNumber}) AS startDate, 20 AS baselineYears
    #     WITH startDate, range(1, baselineYears) as baselineRange
    #     MATCH (country:Country {name: "Australia" })
    #     MATCH (country)<-[:IN]-(report:Report)
    #     WHERE report.flunetRow <> 13112
    #     UNWIND baselineRange AS yearDiff
    #     WITH country, report, startDate, (startDate - duration({years: yearDiff})) AS date
    #     WHERE date - duration({days: 15}) < report.start < date + duration({days: 15})
    #     WITH startDate, avg(report.positive) AS baseline, stDev(report.positive) AS dev
    #     MATCH (country:Country {name: "Australia" })<-[:IN]-(report:Report)
    #     WHERE report.start < startDate < report.start + report.duration AND report.flunetRow <> 13112
    #     WITH startDate, baseline, dev, report.positive AS day
    #     WITH startDate, baseline, day, (day - baseline)/dev AS `Standard deviations from 3-year baseline`
    #     return startDate, day, baseline, `Standard deviations from 3-year baseline`
    #     """
    # )

    # ten_year = session.run(
    #     """
    #     WITH range(0,100) AS weekNumberList UNWIND weekNumberList AS weekNumber
    #     WITH date("2020-01-01") + duration({weeks: weekNumber}) AS startDate, 3 AS baselineYears
    #     WITH startDate, range(1, baselineYears) as baselineRange
    #     MATCH (country:Country {name: "Australia" })
    #     MATCH (country)<-[:IN]-(report:Report)
    #     UNWIND baselineRange AS yearDiff
    #     WITH country, report, startDate, (startDate - duration({years: yearDiff})) AS date
    #     WHERE date - duration({days: 15}) < report.start < date + duration({days: 15})
    #     WITH startDate, avg(report.positive) AS baseline, stDev(report.positive) AS dev
    #     MATCH (country:Country {name: "Australia" })<-[:IN]-(report:Report)
    #     WHERE report.start < startDate < report.start + report.duration
    #     WITH startDate, baseline, dev, report.positive AS day
    #     WITH startDate, baseline, day, (day - baseline)/dev AS `standard deviations from 10 year monthly baseline`
    #     return startDate, day, baseline, `standard deviations from 10 year monthly baseline`
    #     """
    # )

    df3 = pd.DataFrame(three_year, columns=three_year.keys())
    df3["startDate"] = df3["startDate"].apply(lambda x: x.to_native())
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
    df3 = df3.drop("Canada", axis=1)
    df3 = df3.drop("Oman", axis=1)
    # df3 = df3.drop("South Africa", axis=1)

    print(df3)

    df4 = pd.DataFrame()
    for column in df3.columns:
        df4[column] = df3[column].rolling(window=4, center=True).mean()
    # df10 = pd.DataFrame(ten_year, columns=ten_year.keys())
    # df10["startDate"] = df10["startDate"].apply(lambda x: x.to_native())

sns.set()

custom_params = {
    "axes.spines.right": False,
    "axes.spines.top": False,
    # "axes.spines.bottom": False,
    # "axes.bottom": False,
}
sns.set_theme(style="ticks", rc=custom_params)
fig, ax = plt.subplots()


# ax = df10.plot(
#     x="startDate",
#     y="standard deviations from 10 year monthly baseline",
#     linewidth=2,
#     # figsize=(9, 6),
# )
# ax = df3.plot(
#     # x="startDate",
#     # y="Standard deviations from 3-year baseline",
#     # color=df3.columns,
#     figsize=(10, 7),
#     linewidth=1,
#     ax=ax,
# )

df4.plot(
    # x="startDate",
    # y="Standard deviations from 3-year baseline",
    # color=df3.columns,
    figsize=(10, 7),
    linewidth=1.5,
    ax=ax,
)

# sns.pointplot(
#     data=df3, x="startDate", y="Standard deviations from 3-year baseline", ax=ax
# )


ax.spines["bottom"].set_position("zero")
ax.tick_params(bottom=False)
ax.tick_params(axis="x", pad=80)  # 220 # 110 # 180
ax.set(xlabel=None, ylabel=None, title="Influenza deviation from baseline")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))


# ax.spines["bottom"].set_position(("data", 0.0))
# ax.labels["bottom"].set_position(("bottom", 0.0))


NEO4J_DRIVER.close()
plt.show()


# plt.savefig("complete countries no Australia.png", dpi=600)


#     result = session.run(
#         """"
# WITH range(0,52) AS weekNumberList UNWIND weekNumberList AS weekNumber
# WITH date("2020-01-01") + duration({weeks: weekNumber}) AS startDate, 10 AS baselineYears
# WITH startDate, range(1, baselineYears) as baselineRange
# MATCH (country:Country {name: "Australia" })
# MATCH (country)<-[:IN]-(report:Report)
# UNWIND baselineRange AS yearDiff
# WITH country, report, startDate, (startDate - duration({years: yearDiff})) AS date
# WHERE date - duration({days: 15}) < report.start < date + duration({days: 15})
# WITH startDate, avg(report.positive) AS baseline
# MATCH (country:Country {name: "Australia" })<-[:IN]-(report:Report)
# WHERE report.start < startDate < report.start + report.duration
# WITH startDate, baseline, report.positive AS day
# WITH startDate, baseline, day, day - baseline AS standard deviations from
# return startDate, day, baseline, standard deviations from
#         """
#     )

# WITH range(0,52) AS weekNumberList UNWIND weekNumberList AS weekNumber
# WITH date("2020-01-01") + duration({weeks: weekNumber}) AS startDate, 10 AS baselineYears
# WITH startDate, range(1, baselineYears) as baselineRange
# MATCH (country:Country {name: "Australia" })
# MATCH (country)<-[:IN]-(report:Report)
# UNWIND baselineRange AS yearDiff
# WITH country, report, startDate, (startDate - duration({years: yearDiff})) AS date
# WHERE date - duration({days: 15}) < report.start < date + duration({days: 15})
# WITH startDate, avg(toFloat(report.positive) / report.processed) AS baseline
# MATCH (country:Country {name: "Australia" })<-[:IN]-(report:Report)
# WHERE report.start < startDate < report.start + report.duration
# WITH startDate, baseline, toFloat(report.positive) / report.processed AS day
# WITH startDate, baseline, day, day - baseline AS standard deviations from
# return startDate, day, baseline, standard deviations from
