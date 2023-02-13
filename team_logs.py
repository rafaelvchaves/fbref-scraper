'''
This script fetches the most up-to-date match logs for every player in the
players_2022-23.csv file.
'''

import pandas as pd
import datetime
import urllib
import time
import random
import requests
import os



def fetch_team_stats(team):
    name = team['short_name']
    fpl_id = team['id']
    fbref_id = team['fbref_id']
    team_file = f'data/team_logs/{fpl_id}.csv'
    dfs = []
    url = f'https://fbref.com/en/squads/{fbref_id}/2022-2023/matchlogs/c9/schedule/'
    try:
        df = pd.read_html(url)[0]
        # remove multi-indexed columns
        # df.columns = [c[1] for c in df.columns]
        # df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
        # df = df[~df['Date'].isna()]
        df.drop(['Day', 'Referee', 'Match Report', 'Notes'], axis=1, inplace=True)
        df = df[['Date', 'Round', 'Venue', 'GF', 'GA', 'Opponent', 'xG', 'xGA']]
        df.to_csv(team_file, index=False)
    except Exception as e:
        print(f'stats for team {name} not found: {e}')
        return


def fetch_all_logs():
    teams = pd.read_csv('teams_2022-23.csv')
    for team in teams.to_dict(orient='records'):
        print(team['name'])
        fetch_team_stats(team)

if __name__ == '__main__':
    fetch_all_logs()

# df = pd.read_csv('players_2022-23.csv').set_index('id')
# players = []
# for i in players:
#     print(i)
#     player = df.loc[i].to_dict()
#     player['id'] = i
#     time.sleep(5)
#     fetch_player_stats(player, ['summary', 'possession', 'passing'])
