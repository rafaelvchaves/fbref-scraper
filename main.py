import pandas as pd
import os
from datetime import date
import datetime
import gspread
import argparse
from scipy.stats import poisson

parser = argparse.ArgumentParser()
parser.add_argument(
    '-d', '--date', help='start date for player stats', default='2022-12-26')
parser.add_argument('-p', '--position',
                    help='player position to filter by (A for all)', default='A')

args = parser.parse_args()
position = args.position
start_date = args.date

# load in basic player info
players_df = pd.read_csv('players_2022-23.csv')
teams_df = pd.read_csv('teams_2022-23.csv').set_index('id')
players = {}
for player in players_df.to_dict(orient='records'):
    player['team'] = teams_df.loc[player['team'], 'short_name']
    players[player['id']] = player

# load in match logs for each player
data_dir = 'data/match_logs'
dfs = {}
for file in os.listdir(data_dir):
    player_id = int(file.split('.')[0])
    if position != 'A' and players[player_id]['position'] != position:
        continue
    dfs[player_id] = pd.read_csv(os.path.join(data_dir, file), na_values=[
        'On matchday squad, but did not play'])
stat_cols = ['Min', 'xG', 'npxG', 'xA', 'xAG',
             'Att Pen', 'Sh', 'SoT', 'SCA', 'GCA']
stats = pd.DataFrame(
    columns=['Name', 'Price', 'Team', 'Position', 'Games'] + stat_cols)

# average each players stats since the start date and consolidate data
for player_id, data in dfs.items():
    data = data[~data['Date'].isna()]
    num_games = len(data.loc[(data['Date'] >= start_date) & (data['Min'] > 0)])
    if num_games == 0:
        continue
    i = len(stats.index)
    player_info = players[player_id]
    stats.loc[i, 'Name'] = player_info['web_name']
    stats.loc[i, 'Price'] = player_info['price']
    stats.loc[i, 'Team'] = player_info['team']
    stats.loc[i, 'Position'] = player_info['position']
    stats.loc[i, 'Games'] = num_games
    stats.loc[i, stat_cols] = data.loc[data['Date']
                                       >= start_date, stat_cols].mean()
stats.fillna(0, inplace=True)
stats['xGI'] = stats['xG'] + stats['xA']
stats = stats.round(2)

# export to Google sheets
sheet_id = '1pP4l3CdXZjtuWzh2c_OXC82UYngpqoGW7D-4VwVgIY8'
gc = gspread.service_account()
sh = gc.open_by_key(sheet_id)

# player worksheet
# player_ws = sh.add_worksheet(title=f'Players_{start_date}', rows=300, cols=30)
player_ws = sh.worksheet('Players')
player_ws.clear()
player_ws.freeze(rows=1, cols=5)
player_ws.format('1', {'textFormat': {'bold': True}})
player_ws.update([stats.columns.values.tolist()] + stats.values.tolist())

# load in basic player info
teams_df = pd.read_csv('teams_2022-23.csv')
teams = {}
for team in teams_df.to_dict(orient='records'):
    teams[team['id']] = team

# load in match logs for each team
data_dir = 'data/team_logs'
dfs = {}
for file in os.listdir(data_dir):
    team_id = int(file.split('.')[0])
    dfs[team_id] = pd.read_csv(os.path.join(data_dir, file))

stat_cols = ['xG', 'xGA']
home_stats = pd.DataFrame(columns=['Team'] + stat_cols)
away_stats = pd.DataFrame(columns=['Team'] + stat_cols)

for team_id, data in dfs.items():
    i = len(home_stats.index)
    team_info = teams[team_id]
    home_stats.loc[i, 'Team'] = team_info['name']
    home_stats.loc[i, stat_cols] = data.loc[(data['Date']
                                             >= start_date) & (data['Venue'] == 'Home'), stat_cols].mean()
    away_stats.loc[i, 'Team'] = team_info['name']
    away_stats.loc[i, stat_cols] = data.loc[(data['Date']
                                             >= start_date) & (data['Venue'] == 'Away'), stat_cols].mean()

# team home worksheet
team_home_ws = sh.worksheet('Teams (Home)')
team_home_ws.clear()
team_home_ws.freeze(rows=1, cols=1)
team_home_ws.format('1', {'textFormat': {'bold': True}})
team_home_ws.update([home_stats.columns.values.tolist()] +
                    home_stats.values.tolist())

# team away worksheet
team_away_ws = sh.worksheet('Teams (Away)')
team_away_ws.clear()
team_away_ws.freeze(rows=1, cols=1)
team_away_ws.format('1', {'textFormat': {'bold': True}})
team_away_ws.update([away_stats.columns.values.tolist()] +
                    away_stats.values.tolist())
