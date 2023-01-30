'''
This script retrieves a list of all current FPL players and scrapes fbref
to find their fbref ID. The resulting data is placed in the players_{season}.csv
file.
'''

import urllib.request
from bs4 import BeautifulSoup
import requests
import pandas as pd
import json
import os
import time
import unidecode
import requests


HTML_CACHE_DIR = 'data/squads'


def clean(name):
    return unidecode.unidecode(name).lower().replace('-', ' ')


def matches(name, names):
    for candidate_name in names:
        if clean(name) == clean(candidate_name):
            return True
    return False


def check_cache(name):
    cache_file = os.path.join(HTML_CACHE_DIR, f'{name}.html')
    if not os.path.exists(cache_file):
        return None, False
    with open(cache_file, 'rb') as f:
        return f.read(), True


def write_cache(name, html):
    cache_file = os.path.join(HTML_CACHE_DIR, f'{name}.html')
    with open(cache_file, 'wb') as f:
        f.write(html)


def fetch(url, cache_file, cache=True):
    if cache:
        html, ok = check_cache(cache_file)
        if ok:
            return html, True
    response = requests.get(url)
    if response.status_code == 429:
        print(f'Too many requests sent to {url}')
        return None, False
    html = response.content
    write_cache(cache_file, html)
    return html, True


def candidate_names(player):
    return [
        player['web_name'],
        player['first_name'] + ' ' + player['second_name'],
        player['first_name'] + ' ' + player['web_name']
    ]


team_ids = {
    1: '18bb7c10',   # Arsenal
    2: '8602292d',   # Aston Villa
    3: '4ba7cbea',   # Bournemouth
    4: 'cd051869',   # Brentford
    5: 'd07537b9',   # Brighton
    6: 'cff3d9bb',   # Chelsea
    7: '47c64c55',   # Crystal Palace
    8: 'd3fd31cc',   # Everton
    9: 'fd962109',   # Fulham
    10: 'a2d435b3',  # Leicester
    11: '5bfb9659',  # Leeds
    12: '822bd0ba',  # Liverpool
    13: 'b8fd03ef',  # Manchester City
    14: '19538871',  # Manchester United
    15: 'b2b47a98',  # Newcastle United
    16: 'e4a775cb',  # Nottingham Forest
    17: '33c895d4',  # Southampton
    18: '361ca564',  # Tottenham
    19: '7c21e445',  # West Ham
    20: '8cec06e1'   # Wolves
}


def get_team_players(team_id, season):
    squad = {}
    team_fbref_id = team_ids[team_id]
    url = f'https://fbref.com/en/squads/{team_fbref_id}/{season}/'
    html, ok = fetch(url, f'squad-{team_id}-{season}')
    if not ok:
        print('could not fetch html')
        exit(1)
    bs = BeautifulSoup(html, 'html.parser')
    table = bs.find('tbody').find_all('th')
    for row in table:
        try:
            fbref_id, name = row['data-append-csv'], row.find('a').text
            squad[fbref_id] = name
        except:
            continue
    return squad


hardcoded_conversions = {
    1: '839c14e1',    # Cedric Soares
    10: '35e413f1',   # Ben White
    16: '67ac5bb8',   # Gabriel Magalhaes
    655: 'aa81d8f8',  # Karl Hein
    668: '04eb7d82',  # Amario Cozier-Duberry
    685: '3a686640',  # Nathan Butler-Oyedeji
    42: '66b76d44',   # Emiliano Buendia
    60: 'dc4cae05',   # David Brooks
    543: '8ea2227a',  # Matthew Clarke
    684: 'e82900ef',  # David Datro Fofana
    125: '9cfbad36',  # Julio Enciso
    172: '45685411',  # Jesuran Rak-Sakyi
    194: '30d4a2e5',  # Vitaliy Mykolenko
    611: '72c812f3',  # Idrissa Gueye
    205: '0f7533cd',  # Bobby Decordova-Reid
    346: '6639e500',  # Andreas Pereira
    244: '39769cff',  # Rasmus Kristensen
    292: 'f315ca93',  # Kostas Tsimikas
    297: '4d77b365',  # Darwin Nunez
    320: '387e1d35',  # Luke Mbete
    395: '10ec4169',  # Alexander Mighten
    681: '2964fd20',  # Gustavo Scarpa
    415: '5e105217',  # William Smallbone
    250: '01226327',  # Ryan Bertrand
    419: 'afed6722',  # Valentino Livramento
    639: '9f684e25',  # Sammy Braybrooke
    433: '8b04d6c1',  # Pierre-Emile Hojbjerg
    445: 'df8b52a5',  # Emerson Royal
    572: 'ab77d10d',  # Armstrong Oko-Flex
    477: 'e28868f3',  # Jonny Castro
    491: '9a28eba4',  # Adama Traore
    503: 'f5a00fa4',  # Joao Moutinho
    619: '75fdd638',  # Degnand Gnoto
    659: '6ad43f9a',  # Mateo Fernandez
    286: '7a11550b',  # Joe Gomez
    618: '8b529245',  # Carlos Vinicius
    277: '77e84962',  # Thiago Alcantara
    311: '3eb22ec9',  # Bernardo Silva
}


def get_fbref_id(player, season):
    if player['id'] in hardcoded_conversions:
        return hardcoded_conversions[player['id']]
    squad_players = get_team_players(player['team'], season)
    names = candidate_names(player)
    # look for the player on their squad roster
    for fbref_id, name in squad_players.items():
        if matches(name, names):
            return fbref_id
    return None


def get_players():
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    res = requests.get(url).json()
    return res['elements']


def fetch_player_ids(season):
    to_position = {1: 'G', 2: 'D', 3: 'M', 4: 'F'}
    # get FPL player data
    df = pd.DataFrame(get_players())
    # filter out players with < 270 minutes, and goalkeepers
    df = df[(df['minutes'] >= 270) & (df['element_type'] != 1)]
    df['position'] = df['element_type'].apply(lambda x: to_position[x])
    df = df[['id', 'first_name', 'second_name', 'web_name', 'team', 'position']]
    data = []
    for player in df.to_dict(orient='records'):
        # try to find corresponding fbref id for each player
        player['fbref_id'] = get_fbref_id(player, season)
        data.append(player)
    print('Number of players:', len(data))
    pd.DataFrame(data).to_csv(f'players_{season}.csv', index=False)

def get_teams():
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    res = requests.get(url).json()
    return res['teams']

def fetch_team_ids(season):
    df = pd.DataFrame(get_teams())
    df['fbref_id'] = df['id'].apply(lambda i : team_ids[i])
    # df = df.set_index('id')
    df.to_csv(f'teams_{season}.csv', index=False)

# fetch_player_ids('2022-23')
fetch_team_ids('2022-23')
