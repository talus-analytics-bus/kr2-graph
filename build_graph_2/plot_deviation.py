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
        WITH range(0,104) AS weekNumberList UNWIND weekNumberList AS weekNumber
        WITH date("2020-01-01") + duration({weeks: weekNumber}) AS startDate, 20 AS baselineYears
        WITH startDate, range(1, baselineYears) as baselineRange
        MATCH (country:Country {name: "Australia" })
        MATCH (country)<-[:IN]-(report:Report)
        WHERE report.flunetRow <> 13112
        UNWIND baselineRange AS yearDiff
        WITH country, report, startDate, (startDate - duration({years: yearDiff})) AS date 
        WHERE date - duration({days: 15}) < report.start < date + duration({days: 15})
        WITH startDate, avg(report.positive) AS baseline, stDev(report.positive) AS dev
        MATCH (country:Country {name: "Australia" })<-[:IN]-(report:Report)
        WHERE report.start < startDate < report.start + report.duration AND report.flunetRow <> 13112
        WITH startDate, baseline, dev, report.positive AS day
        WITH startDate, baseline, day, (day - baseline)/dev AS `Standard deviations from 3-year baseline`
        return startDate, day, baseline, `Standard deviations from 3-year baseline`
        """
    )

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


# ax = df10.plot(
#     x="startDate",
#     y="standard deviations from 10 year monthly baseline",
#     linewidth=2,
#     # figsize=(9, 6),
# )
ax = df3.plot(
    x="startDate",
    y="Standard deviations from 3-year baseline",
    # figsize=(10, 7),
    linewidth=1,
    # ax=ax,
)

sns.pointplot(
    data=df3, x="startDate", y="Standard deviations from 3-year baseline", ax=ax
)


ax.spines["bottom"].set_position("zero")
ax.tick_params(bottom=False)
ax.tick_params(axis="x", pad=180)  # 220 # 110 # 180
ax.set(xlabel=None, ylabel=None, title="Influenza in Australia")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))


# ax.spines["bottom"].set_position(("data", 0.0))
# ax.labels["bottom"].set_position(("bottom", 0.0))


NEO4J_DRIVER.close()
plt.show()
# plt.savefig("three year baseline for deck.png", dpi=600)


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
