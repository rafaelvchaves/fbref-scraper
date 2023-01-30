import pandas as pd
import os
from datetime import date
import datetime
import gspread
import argparse

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

# stat_cols = ['Min', 'Gls', 'Ast', 'PK', 'PKatt', 'Sh', 'SoT', 'CrdY', 'CrdR', 'Touches', 'Tkl', 'Int', 'Blocks', 'xG', 'npxG', 'xAG', 'SCA',
#              'GCA', 'Cmp', 'Att', 'Cmp%', 'Prog', 'Succ', 'Def Pen', 'Def 3rd', 'Mid 3rd', 'Att 3rd', 'Att Pen', 'Live', 'Succ%', 'Mis', 'Dis', 'Rec']
stat_cols = ['Min', 'Sh', 'SoT', 'xG', 'npxG',
             'xAG', 'SCA', 'GCA', 'Att 3rd', 'Att Pen', 'xA']
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
# stats_df = stats[stat_cols]
# stats[stat_cols] = (stats_df - stats_df.min()) / \
#     (stats_df.max() - stats_df.min())
# stats['Test'] = stats['npxG'] + stats['xA'] + \
#     stats['Sh'] + stats['Att Pen'] + stats['GCA']
stats = stats.round(2)

# export to Google sheets
sheet_id = '1pP4l3CdXZjtuWzh2c_OXC82UYngpqoGW7D-4VwVgIY8'
gc = gspread.service_account()
sh = gc.open_by_key(sheet_id)
ws = sh.worksheet('12/26')
ws.clear()
ws.format('1', {'textFormat': {'bold': True}})
ws.update([stats.columns.values.tolist()] + stats.values.tolist())
