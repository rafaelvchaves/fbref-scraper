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

calls = 0
failures = []


def fetch_player_stats(player, stats_type=['summary']):
    global calls
    name = player['web_name']
    fbref_id = player['fbref_id']
    fpl_id = player['id']
    player_file = f'data/match_logs/{fpl_id}.csv'
    dfs = []
    for st in stats_type:
        time.sleep(1)
        url = f'https://fbref.com/en/players/{fbref_id}/matchlogs/2022-2023/c9/{st}/'
        try:
            calls += 1
            df = pd.read_html(url)[0]
            # remove multi-indexed columns
            df.columns = [c[1] for c in df.columns]
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
            df = df[~df['Date'].isna()]
            df.drop(['Day', 'Pos', 'Match Report'], axis=1, inplace=True)
            # if os.path.exists(player_file):
            #     prev_df = pd.read_csv(player_file)
            #     if len(prev_df) == len(df):
            #         # already up-to-date, no need to make more calls
            #         print(f'skipping {name}...')
            #         return
            dfs.append(df)
        except Exception as e:
            failures.append(fpl_id)
            print(f'{st} stats for player {name} not found: {e}')
            return
    if len(dfs) == 0:
        return
    df = pd.concat(dfs, axis=1)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    df = df.replace('On matchday squad, but did not play', float('nan'))
    df.to_csv(player_file, index=False)


def fetch_all_logs():
    players = pd.read_csv('players_2022-23.csv')
    data = []
    # handle fbref rate limit of 20 requests per minute
    start = time.perf_counter()
    log_types = ['summary', 'possession', 'passing']
    for player in players.to_dict(orient='records'):
        print(player['web_name'])
        fetch_player_stats(player, log_types)
        if calls >= 18:
            elapsed = time.perf_counter() - start
            time.sleep(max(0, 60 - elapsed))
            calls = 0
            start = time.perf_counter()
    print(failures)

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
