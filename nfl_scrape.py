# Scraping modules
from urllib.request import urlopen
from bs4 import BeautifulSoup as bs

# Data manipulation modules
import numpy as np
import pandas as pd

# Visualization modules
import matplotlib as mpl
import matplotlib.pyplot as plt

# Relevant URL
url = "https://www.pro-football-reference.com/years/2021/passing.htm"

# Open URL and pass to BeautifulSoup
html = urlopen(url)
stats_page = bs(html, features="html.parser")

# Get table headers
column_headers = stats_page.findAll("tr")[0]
column_headers = [i.getText() for i in column_headers.findAll("th")]

# print(column_headers)

# Get table rows
rows = stats_page.findAll("tr")[1:]

# Get stats from each row
qb_stats = []
for i in range(len(rows)):
    qb_stats.append([col.getText() for col in rows[i].findAll("td")])

# print(qb_stats[0])

# Created pandas DataFrame
data = pd.DataFrame(qb_stats, columns=column_headers[1:])

# Rename sack yards column
new_columns = data.columns.values
new_columns[-6] = "Sk_Yds"
data.columns = new_columns

# print(data.head())

# Select stat categories
categories = ["Cmp%", "Yds", "TD", "Int", "Y/A", "Rate"]

# Subset for radar chart
data_radar = data.loc[:, ["Player", "Tm"] + categories]
# print(data_radar.head())

# Convert to numerical values
for cat in categories:
    data_radar[cat] = pd.to_numeric(data[cat])

# print(data_radar.head())
# Check data types
# print(data_radar.dtypes)

# Remove ornamental characters
data_radar["Player"] = data_radar["Player"].str.replace('*', '', regex=True)
data_radar["Player"] = data_radar["Player"].str.replace('+', '', regex=True)

# Filter by passing yards
data_radar_filtered = data_radar.loc[data_radar.loc[:, "Yds"] > 1500, :]

# Create columns with percentile rank
for cat in categories:
    data_radar_filtered.loc[:, cat + "_Rank"] = data_radar_filtered.loc[:, cat].rank(pct=True)

# Flip rank for interceptions
data_radar_filtered.loc[:, "Int_Rank"] = 1 - data_radar_filtered.loc[:, "Int_Rank"]

# Examine data
# print(data_radar_filtered.head())

# General plot parameters
mpl.rcParams["font.family"] = "Avenir"
mpl.rcParams["font.size"] = 16
mpl.rcParams["axes.linewidth"] = 0
mpl.rcParams["xtick.major.pad"] = 15

# Team Colors
team_colors = {
    'ARI':'#97233f', 'ATL':'#a71930', 'BAL':'#241773', 'BUF':'#00338d',
    'CAR':'#0085ca', 'CHI':'#0b162a', 'CIN':'#fb4f14', 'CLE':'#311d00',
    'DAL':'#041e42', 'DEN':'#002244', 'DET':'#0076b6', 'GNB':'#203731',
    'HOU':'#03202f', 'IND':'#002c5f', 'JAX':'#006778', 'KAN':'#e31837',
    'LAC':'#002a5e', 'LAR':'#003594', 'MIA':'#008e97', 'MIN':'#4f2683',
    'NWE':'#002244', 'NOR':'#d3bc8d', 'NYG':'#0b2265', 'NYJ':'#125740',
    'OAK':'#000000', 'PHI':'#004c54', 'PIT':'#ffb612', 'SFO':'#aa0000',
    'SEA':'#002244', 'TAM':'#d50a0a', 'TEN':'#0c2340', 'WAS':'#773141'
}

# Calculate angles for radar chart
offset = np.pi/6
angles = np.linspace(0, 2*np.pi, len(categories) + 1) + offset

def create_radar_chart(ax, angles, player_data, color="blue"):
    # Plot data and fill with team color
    ax.plot(angles, np.append(player_data[-(len(angles)-1):],
            player_data[-(len(angles)-1)]), color=color, linewidth=2)
    ax.fill(angles, np.append(player_data[-(len(angles)-1):],
            player_data[-(len(angles)-1)]), color=color, alpha=0.2)

    # Set category labels
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories)

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

def get_qb_data(data, team):
    return np.asarray(data[data['Tm'] == team])[0]

# Create figure
fig = plt.figure(figsize=(8, 8), facecolor="white")

# Add subplots
ax1 = fig.add_subplot(221, projection="polar", facecolor="#ededed")
ax2 = fig.add_subplot(222, projection="polar", facecolor="#ededed")
ax3 = fig.add_subplot(223, projection="polar", facecolor="#ededed")
ax4 = fig.add_subplot(224, projection="polar", facecolor="#ededed")

# Adjust space between subplots
plt.subplots_adjust(hspace=0.8, wspace=0.5)

# Get QB data
sf_data = get_qb_data(data_radar_filtered, "SFO")
sea_data = get_qb_data(data_radar_filtered, "SEA")
ari_data = get_qb_data(data_radar_filtered, "ARI")
lar_data = get_qb_data(data_radar_filtered, "LAR")

# Plot QB data
ax1 = create_radar_chart(ax1, angles, lar_data, team_colors["LAR"])
ax2 = create_radar_chart(ax2, angles, ari_data, team_colors["ARI"])
ax3 = create_radar_chart(ax3, angles, sea_data, team_colors["SEA"])
ax4 = create_radar_chart(ax4, angles, sf_data, team_colors["SFO"])

plt.show()