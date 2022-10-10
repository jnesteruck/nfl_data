'''
nfl.py - modifications by Jacob Nesteruck

Project based on tutorial created by Naveen Venkatesan (https://towardsdatascience.com/scraping-nfl-stats-to-compare-quarterback-efficiencies-4989642e02fe).

Scrapes Pro-Football-Reference website for individual NFL player defensive stats.

All data belongs to Pro-Football-Reference (https://www.pro-football-reference.com/)

'''

# Scraping modules
from typing import Union
from urllib.request import urlopen
from bs4 import BeautifulSoup as bs

# Data manipulation modules
import numpy as np
import pandas as pd

# Visualization modules
import matplotlib as mpl
import matplotlib.pyplot as plt

'''
URL Components: https://www.pro-football-reference.com/years/{YEAR}/{CATEGORY}.htm

YEAR - Can be any year for which Pro Football Reference has the data
CATEGORY - One of the following: passing, rushing, recieving, scrimmage, defense, kicking, returns, scoring

NOTES ABOUT HISTORICAL DATA:
    - Only Scoring stats are tracked before the 1932 season
    - Many defensive stats only officially date back 30-40 years, unofficially 60 years
        - Passes defended (PD) have been tracked since 1999
        - Fumbles have been reliably tracked since 1990
        - Sacks have been official since 1982, unofficially tallied back to 1960 through study of historical game film
        - Tackle data prior to 1994 is not specified beyond combined tally. Solo and Assisted tackles have been recorded since 1994.
          Even so, record-keeping on tackles is still unofficial and somewhat unreliable to this day.
        - Tackles for Loss have only been reliably tracked since 2008, recorded for about 95% of games between 1999 and 2007
        - No QB Hits data prior to 2006

'''
# PRO-FOOTBALL-REFERENCE INDIVIDUAL STATISTICAL CATEGORIES
STAT_CATEGORIES = [
    "passing",
    "rushing",
    "recieving",
    "scrimmage",
    "defense",
    "kicking",
    "returns",
    "scoring"
]

# Relevant URL
url = "https://www.pro-football-reference.com/years/2021/defense.htm"

# Open URL and pass to BeautifulSoup
html = urlopen(url)
stats_page = bs(html, features="html.parser")

# Get table headers
column_headers = stats_page.findAll("tr")[1]
column_headers = [i.getText() for i in column_headers.findAll("th")]

# print(column_headers) # Check column_headers

# Get table rows
rows = stats_page.findAll("tr")[2:]

# Get stats from each row
qb_stats = []
for i in range(len(rows)):
    qb_stats.append([col.getText() for col in rows[i].findAll("td")])

# print(qb_stats[0])

# Created pandas DataFrame
data = pd.DataFrame(qb_stats, columns=column_headers[1:])

# Rename Yard and TD columns
new_columns = data.columns.values
new_columns[7] = "IntYds"
new_columns[8] = "IntTD"
new_columns[-9] = "FmbYds"
new_columns[-8] = "FmbTD"
data.columns = new_columns

# Replace Null with 0
data.replace("", 0, inplace=True)

# Remove ornamental characters
data["Player"] = data["Player"].str.replace('*', '', regex=True)
data["Player"] = data["Player"].str.replace('+', '', regex=True)

# Select stat categories
DBcategories = ["Int", "IntTD", "PD", "Sk", "Comb", "Solo"]
F7categories = ["PD", "FF", "Comb", "Sk", "TFL", "QBHits"]
allcategories = DBcategories + F7categories

# Convert to numerical values
for cat in allcategories:
    data[cat] = pd.to_numeric(data[cat])
# Subset for radar chart
data_radarDB = data[["Player", "Tm"] + DBcategories]
# print(data_radarDB.head())

data_radarF7 = data[["Player", "Tm"] + F7categories]
# print(data_radarF7.head())

# Check data types
# print(data_radarDB.dtypes)
# print(data_radarF7.dtypes)

# Sort values in each DF
data_radarDB.sort_values(["Int"], ascending=False, inplace=True)
data_radarF7.sort_values(["Sk"], ascending=False, inplace=True)


# print(data_radarDB.head())
# print(data_radarF7.head())

# Filter by Solo tackles, Sacks
data_radar_filteredDB = data_radarDB.loc[data_radarDB["Int"] >= 5]
data_radar_filteredF7 = data_radarF7.loc[data_radarF7["Sk"] >= 12]
# data_radar_filteredDB.sort_values(["Int", "PD", "Int TD"], ascending=False, inplace=True)
# data_radar_filteredF7.sort_values(["Sk", "FF", "QBHits"], ascending=False, inplace=True)

# Create columns with percentile rank
# for cat in DBcategories:
#     data_radar_filteredDB.loc[:, cat + "_Rank"] = data_radar_filteredDB.loc[:, cat].rank(pct=True)
# for cat in F7categories:
#     data_radar_filteredF7.loc[:, cat + "_Rank"] = data_radar_filteredF7.loc[:, cat].rank(pct=True)

# Examine data
# print(data_radar_filteredDB.head())
# print(data_radar_filteredF7.head())

# General plot parameters
# mpl.rcParams["font.family"] = "Avenir"
mpl.rcParams["font.size"] = 12
mpl.rcParams["axes.linewidth"] = 0.5
# mpl.rcParams["xtick.major.pad"] = 15

# Team Colors
team_colors = {
    'ARI':'#97233f', 'ATL':'#a71930', 'BAL':'#241773', 'BUF':'#00338d',
    'CAR':'#0085ca', 'CHI':'#0b162a', 'CIN':'#fb4f14', 'CLE':'#311d00',
    'DAL':'#041E42', 'DEN':'#fa4616', 'DET':'#0076b6', 'GNB':'#203731',
    'HOU':'#03202f', 'IND':'#002c5f', 'JAX':'#006778', 'KAN':'#e31837',
    'LAC':'#002a5e', 'LAR':'#003594', 'MIA':'#008e97', 'MIN':'#4f2683',
    'NWE':'#b0b7bc', 'NOR':'#d3bc8d', 'NYG':'#0b2265', 'NYJ':'#125740',
    'LVR':'#000000', 'PHI':'#004c54', 'PIT':'#ffb612', 'SFO':'#aa0000',
    'SEA':'#69be28', 'TAM':'#d50a0a', 'TEN':'#0c2340', 'WAS':'#773141'
}

'''# Calculate angles for radar chart
offset = np.pi/6
DBangles = np.linspace(0, 2*np.pi, len(DBcategories) + 1) + offset
F7angles = np.linspace(0, 2*np.pi, len(DBcategories) + 1) + offset

# define create_radar_chart function
def create_radar_chart(ax, angles, player_data, labels, color="blue"):
    # Plot data and fill with team color
    ax.plot(angles, np.append(player_data[-(len(angles)-1):],
            player_data[-(len(angles)-1)]), color=color, linewidth=2)
    ax.fill(angles, np.append(player_data[-(len(angles)-1):],
            player_data[-(len(angles)-1)]), color=color, alpha=0.2)

    # Set category labels
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(labels)

    # Remove radial labels
    ax.set_yticklabels([])

    # Add Player names
    ax.text(np.pi/2, 1.7, player_data[0], ha="center",
            va="center", size=18, color=color)
    
    # Use white grid
    ax.grid(color="white", linewidth=1.5)

    # Set axis limits
    ax.set(xlim=(0, 2*np.pi), ylim=(0, 1))

    return ax

def get_team_data(data, team):
    return np.asarray(data[data['Tm'] == team])[0]

def get_ranked_data(data, rank):
    return np.asarray(data)[0:rank]

# Create DB figure
figDB = plt.figure(figsize=(8, 8), facecolor="white")

# Add DB subplots
DBax1 = figDB.add_subplot(221, projection="polar", facecolor="#ededed")
DBax2 = figDB.add_subplot(222, projection="polar", facecolor="#ededed")
DBax3 = figDB.add_subplot(223, projection="polar", facecolor="#ededed")
DBax4 = figDB.add_subplot(224, projection="polar", facecolor="#ededed")
# Adjust space between subplots
plt.subplots_adjust(hspace=0.8, wspace=0.5)

# Create F7 figure
figF7 = plt.figure(figsize=(8, 8), facecolor="white")
# Add F7 subplots
F7ax1 = figF7.add_subplot(221, projection="polar", facecolor="#ededed")
F7ax2 = figF7.add_subplot(222, projection="polar", facecolor="#ededed")
F7ax3 = figF7.add_subplot(223, projection="polar", facecolor="#ededed")
F7ax4 = figF7.add_subplot(224, projection="polar", facecolor="#ededed")
# Adjust space between subplots
plt.subplots_adjust(hspace=0.8, wspace=0.5)

# Get Top 4 DBs
db1, db2, db3, db4 = get_ranked_data(data_radar_filteredDB, 4)
# Top 4 F7s
f71, f72, f73, f74 = get_ranked_data(data_radar_filteredF7, 4)

# Plot DB data
DBax1 = create_radar_chart(DBax1, DBangles, db1, DBcategories, team_colors[db1[1]])
DBax2 = create_radar_chart(DBax2, DBangles, db2, DBcategories, team_colors[db2[1]])
DBax3 = create_radar_chart(DBax3, DBangles, db3, DBcategories, team_colors[db3[1]])
DBax4 = create_radar_chart(DBax4, DBangles, db4, DBcategories, team_colors[db4[1]])
# Plot F7 data
F7ax1 = create_radar_chart(F7ax1, F7angles, f71, F7categories, team_colors[f71[1]])
F7ax2 = create_radar_chart(F7ax2, F7angles, f72, F7categories, team_colors[f72[1]])
F7ax3 = create_radar_chart(F7ax3, F7angles, f73, F7categories, team_colors[f73[1]])
F7ax4 = create_radar_chart(F7ax4, F7angles, f74, F7categories, team_colors[f74[1]])'''

# Show Sack leaders
colors = [team_colors[tm] for tm in list(data_radar_filteredF7["Tm"])]
axSk = data_radarF7.head(10).plot.bar(x="Player", y="Sk", figsize=(10,7), color=colors, title="2021 NFL Sack Leaders", legend=False)
plt.xticks(rotation=30, ha="right")
for p in axSk.patches:
    width = p.get_width()
    height = p.get_height()
    pheight = height
    if height < 0:
        pheight = height - 7*10**5
    x, y = p.get_xy()
    axSk.annotate(f'{height:,.1f}', (x + width/2, y + pheight*1.02), va='bottom', ha='center', fontsize=12)
plt.ylim(top=24.9)
plt.ylabel("Sacks")
plt.xlabel("")
plt.tight_layout()

# Show Int leaders
colors = [team_colors[tm] for tm in list(data_radar_filteredDB["Tm"])]
axSk = data_radarDB.head(10).plot.bar(x="Player", y="Int", figsize=(10,7), color=colors, title="2021 NFL Interception Leaders", legend=False)
plt.xticks(rotation=30, ha="right")
for p in axSk.patches:
    width = p.get_width()
    height = p.get_height()
    pheight = height
    if height < 0:
        pheight = height - 7*10**5
    x, y = p.get_xy()
    axSk.annotate(f'{height:,.0f}', (x + width/2, y + pheight*1.02), va='bottom', ha='center', fontsize=12)
plt.ylim(top=11.9)
plt.ylabel("Interceptions")
plt.xlabel("")
plt.tight_layout()

'''# Spark session
from pyspark.sql import SparkSession
# from methods import *

# ---------- BUILD SPARK SESSION ---------- #
spark = SparkSession.builder \
    .master("local") \
    .appName("mlb") \
    .getOrCreate()

# ---------- SPARK CONTEXT ---------- #
sc = spark.sparkContext
sc.setLogLevel("WARN")

# ---------- SET FILEPATH ---------- #
# path = "file:/mnt/c/Users/jan94/OneDrive/Documents/code/NFL_PROJECT/Artifacts/"

dataDB = spark.createDataFrame(data_radarDB)
dataF7 = spark.createDataFrame(data_radarF7)

# dataDB.show()
# dataF7.show()
dataDB.createOrReplaceTempView("dataDB")
dataF7.createOrReplaceTempView("dataF7")

dataDB = spark.sql("SELECT * FROM dataDB WHERE Tm IS NOT NULL;")
dataF7 = spark.sql("SELECT * FROM dataF7 WHERE Tm IS NOT NULL;")
dataDB.createOrReplaceTempView("dataDB")
dataF7.createOrReplaceTempView("dataF7")

# DF columns
# DBcategories = ["Player", "Tm", "Int", "IntTD", "PD", "Sk", "Comb", "Solo"]
# F7categories = ["Player", "Tm", "PD", "FF", "Comb", "Sk", "TFL", "QBHits"]

spark.sql("SELECT Tm, SUM(Int) Total_Int, SUM(IntTD) Total_IntTD, SUM(PD) Total_PD, SUM(Sk) Total_Sk, " \
          "SUM(Comb) Total_Comb, SUM(Solo) Total_Solo FROM dataDB WHERE Tm != '2TM' AND Tm != '3TM' " \
          "GROUP BY Tm ORDER BY Total_Int DESC;").write.csv(path + "dataDB", header=True)

spark.sql("SELECT Tm, SUM(Sk) Total_Sk, SUM(FF) Total_FF, SUM(TFL) Total_TFL, SUM(QBHits) Total_QBHits, " \
          "SUM(Comb) Total_Comb, SUM(PD) Total_PD FROM dataF7 WHERE Tm != '2TM' AND Tm != '3TM' "\
          "GROUP BY Tm ORDER BY Total_Sk DESC;").write.csv(path + "dataF7", header=True)

spark.stop()'''

path = "Artifacts/DataFrames/"

data_DB = pd.read_csv(path + "dataDB.csv")
data_F7 = pd.read_csv(path + "dataF7.csv")

# Show Sack leaders
colors = [team_colors[tm] for tm in list(data_F7["Tm"])]
axSk = data_F7.plot.bar(x="Tm", y="Total_Sk", figsize=(16,7), color=colors, title="2021 NFL Sacks by Team", legend=False)
plt.xticks(rotation=0, ha="center")
for p in axSk.patches:
    width = p.get_width()
    height = p.get_height()
    pheight = height
    if height < 0:
        pheight = height - 7*10**5
    x, y = p.get_xy()
    axSk.annotate(f'{height:,.1f}', (x + width/2, y + pheight*1.02), va='bottom', ha='center', fontsize=12)
plt.ylim(top=58)
plt.ylabel("Sacks")
plt.xlabel("")
plt.tight_layout()

# Show Int leaders
colors = [team_colors[tm] for tm in list(data_DB.loc[:, "Tm"])]
axSk = data_DB.plot.bar(x="Tm", y="Total_Int", figsize=(16,7), color=colors, title="2021 NFL Interceptions by Team", legend=False)
plt.xticks(rotation=0, ha="center")
for p in axSk.patches:
    width = p.get_width()
    height = p.get_height()
    pheight = height
    if height < 0:
        pheight = height - 7*10**5
    x, y = p.get_xy()
    axSk.annotate(f'{height:,.0f}', (x + width/2, y + pheight*1.02), va='bottom', ha='center', fontsize=12)
plt.ylim(top=28)
plt.ylabel("Interceptions")
plt.xlabel("")
plt.tight_layout()

# Display charts
plt.show()