from nba_api.stats.endpoints import boxscoresummaryv2
from nba_api.stats.endpoints import boxscoretraditionalv2
from nba_api.stats.endpoints import leaguestandings
from datetime import datetime
import pandas as pd
import json


def get_team_names_and_id(game_id):
    # Ottieni il riepilogo delle statistiche della partita dal game_id
    boxscore_summary = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
    data = boxscore_summary.get_normalized_dict()

    # Estrai i nomi delle squadre di casa e in trasferta
    home_team_name = data['TeamStats'][1]['TEAM_CITY'] + " " + data['TeamStats'][1]['TEAM_NAME']
    id_home_team = data['TeamStats'][1]['TEAM_ABBREVIATION']
    visitor_team_name = data['TeamStats'][0]['TEAM_CITY'] + " " + data['TeamStats'][0]['TEAM_NAME']
    id_visitor_team = data['TeamStats'][0]['TEAM_ABBREVIATION']
    return home_team_name, id_home_team, visitor_team_name, id_visitor_team


def get_officials_by_game_id(game_id):
    boxscore = boxscoresummaryv2.BoxScoreSummaryV2(game_id=game_id)
    data = boxscore.get_normalized_dict()['Officials']
    officials = []
    for i in range(0, 3):
        officials.append(data[i]['FIRST_NAME'] + " " + data[i]['LAST_NAME'])
    return officials


def get_officials_by_game_id_complete(game_id):
    boxscore = boxscoresummaryv2.BoxScoreSummaryV2(game_id=game_id)
    data = boxscore.get_normalized_dict()['Officials']
    officials = []
    for i in range(0, 3):
        officials.append(str(data[i]['OFFICIAL_ID']) + " " + data[i]['FIRST_NAME'] + " " + data[i]['LAST_NAME'] + " " + str(data[i]['JERSEY_NUM']))
    return officials


def get_first_official_by_game_id(game_id): 
    boxscore = boxscoresummaryv2.BoxScoreSummaryV2(game_id=game_id)
    data = boxscore.get_normalized_dict()['Officials'][0]
    first_official = data['FIRST_NAME'] + " " + data['LAST_NAME']
    return first_official

def get_first_official_by_game_id_complete(game_id): 
    boxscore = boxscoresummaryv2.BoxScoreSummaryV2(game_id=game_id)
    data = boxscore.get_normalized_dict()['Officials'][0]
    first_official = str(data['OFFICIAL_ID']) + " " + data['FIRST_NAME'] + " " + data['LAST_NAME'] + " " + str(data['JERSEY_NUM'])
    return first_official

def get_schedule():
    with open('../data/schedule.json') as f:
        data = json.load(f)
    calendar = pd.DataFrame(columns=['game_id', 'home_team_id', 'home_team_tricode', 'home_team_city', 'home_team_name', 'away_team_id', 'away_team_tricode','away_team_city', 'away_team_name', 'datetime'])
    for i in range(0, len(data['leagueSchedule']['gameDates'])):
        data_ = data['leagueSchedule']['gameDates'][i]['games'][0]
        calendar.loc[i] = (data_['gameId'],
                        data_['homeTeam']['teamId'],
                        data_['homeTeam']['teamTricode'],
                        data_['homeTeam']['teamCity'],
                        data_['homeTeam']['teamName'],
                        data_['awayTeam']['teamId'],
                        data_['awayTeam']['teamTricode'],
                        data_['awayTeam']['teamCity'],
                        data_['awayTeam']['teamName'],
                        data_['gameDateTimeUTC']
                        )
    return calendar.drop([178, 179, 180], axis = 0)

def get_future_schedule():
    with open('../data/schedule.json') as f:
        data = json.load(f)
    calendar = pd.DataFrame(columns=['game_id', 'home_team_id', 'home_team_tricode', 'home_team_city', 'home_team_name', 'away_team_id', 'away_team_tricode','away_team_city', 'away_team_name', 'datetime'])
    for i in range(0, len(data['leagueSchedule']['gameDates'])):
        data_ = data['leagueSchedule']['gameDates'][i]['games'][0]
        calendar.loc[i] = (data_['gameId'],
                        data_['homeTeam']['teamId'],
                        data_['homeTeam']['teamTricode'],
                        data_['homeTeam']['teamCity'],
                        data_['homeTeam']['teamName'],
                        data_['awayTeam']['teamId'],
                        data_['awayTeam']['teamTricode'],
                        data_['awayTeam']['teamCity'],
                        data_['awayTeam']['teamName'],
                        data_['gameDateTimeUTC']
                        )
    return calendar[calendar['datetime'] >= datetime.now().strftime("%Y-%m-%d %H:%M:%S")]

def get_standings(conference):
    nba_league_standings = leaguestandings.LeagueStandings().get_data_frames()[0]
    standings = nba_league_standings[nba_league_standings['Conference'] == conference]
    df = pd.DataFrame(columns=['Position', 'TeamName', 'Wins', 'Losses', 'WinPCT', 'PointsPG'])
    for index, row in standings.iterrows():
        df.loc[df.shape[0]] = [df.shape[0] + 1, row['TeamCity'] + " " + row['TeamName'], + row['WINS'], + row['LOSSES'], row['WinPCT'], row['PointsPG']]
    return df