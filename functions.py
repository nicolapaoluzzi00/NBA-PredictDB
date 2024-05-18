from nba_api.stats.endpoints import boxscoresummaryv2
from nba_api.stats.endpoints import boxscoretraditionalv2, leaguegamefinder
from datetime import datetime
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import json
from nba_api.stats.static import teams
from nba_api.stats.endpoints import teamgamelog, leagueleaders, leaguestandings
import numpy as np

def get_officials_by_game_id(game_id):
    boxscore = boxscoresummaryv2.BoxScoreSummaryV2(game_id=game_id)
    data = boxscore.get_normalized_dict()['Officials']
    officials = []
    for i in range(0, 3):
        officials.append(data[i]['FIRST_NAME'] + " " + data[i]['LAST_NAME'])
    return officials

def get_first_official_by_game_id(game_id): 
    boxscore = boxscoresummaryv2.BoxScoreSummaryV2(game_id=game_id)
    data = boxscore.get_normalized_dict()['Officials'][0]
    first_official = data['FIRST_NAME'] + " " + data['LAST_NAME']
    return first_official

def get_schedule():
    with open('./data/schedule.json') as f:
        data = json.load(f)
    calendar = pd.DataFrame(columns=['game_id', 'home_team_id', 'home_team_tricode', 'home_team_city', 'home_team_name', 'away_team_id', 'away_team_tricode','away_team_city', 'away_team_name', 'datetime', "game_label", "arena_name", "arena_city"])
    count = 0
    for i in range(0, len(data['leagueSchedule']['gameDates'])):
        for j in range(0, len(data['leagueSchedule']['gameDates'][i]['games'])):
            data_ = data['leagueSchedule']['gameDates'][i]['games'][j]
            if data_['gameDateTimeUTC'] < '2023-10-24':
                continue
            if data_['gameLabel'] != '':
                continue
            calendar.loc[count] = [data_['gameId'],
                            data_['homeTeam']['teamId'],
                            data_['homeTeam']['teamTricode'],
                            data_['homeTeam']['teamCity'],
                            data_['homeTeam']['teamName'],
                            data_['awayTeam']['teamId'],
                            data_['awayTeam']['teamTricode'],
                            data_['awayTeam']['teamCity'],
                            data_['awayTeam']['teamName'],
                            data_['gameDateTimeUTC'],
                            data_['gameLabel'],
                            data_['arenaName'],
                            data_['arenaCity']]
            count = count + 1
    return calendar[:len(calendar)-6]


def get_rank_players():
    # Ottieni la classifica dei migliori giocatori
    leaders = leagueleaders.LeagueLeaders()

    return leaders.get_data_frames()[0][["PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "PTS"]][:10].to_json(orient="records")

def get_future_schedule():
    with open('./data/schedule.json') as f:
        data = json.load(f)
    calendar = pd.DataFrame(columns=['game_id', 'home_team_id', 'home_team_tricode', 'home_team_city', 'home_team_name', 'away_team_id', 'away_team_tricode','away_team_city', 'away_team_name', 'datetime'])
    count = 0
    for i in range(0, len(data['leagueSchedule']['gameDates'])):
        for j in range(0, len(data['leagueSchedule']['gameDates'][i]['games'])):
            data_ = data['leagueSchedule']['gameDates'][i]['games'][j]
            calendar.loc[count] = (data_['gameId'],
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
            count = count + 1
    return calendar[calendar['datetime'] >= datetime.now().strftime("%Y-%m-%d %H:%M:%S")]

def get_future_schedule_2():
    with open('./data/schedule.json') as f:
        data = json.load(f)
    calendar = pd.DataFrame(columns=['game_id', 'home_team_id', 'home_team_tricode', 'home_team_city', 'home_team_name', 'away_team_id', 'away_team_tricode','away_team_city', 'away_team_name', 'datetime', "game_label", "arena_name", "arena_city"])
    count = 0
    for i in range(0, len(data['leagueSchedule']['gameDates'])):
        for j in range(0, len(data['leagueSchedule']['gameDates'][i]['games'])):
            data_ = data['leagueSchedule']['gameDates'][i]['games'][j]
            calendar.loc[count] = (data_['gameId'],
                            data_['homeTeam']['teamId'],
                            data_['homeTeam']['teamTricode'],
                            data_['homeTeam']['teamCity'],
                            data_['homeTeam']['teamName'],
                            data_['awayTeam']['teamId'],
                            data_['awayTeam']['teamTricode'],
                            data_['awayTeam']['teamCity'],
                            data_['awayTeam']['teamName'],
                            data_['gameDateTimeUTC'],
                            data_['gameLabel'],
                            data_['arenaName'],
                            data_['arenaCity']
                            )
            count = count + 1
    return calendar[calendar['datetime'] >= (datetime(2024, 3, 1)).strftime("%Y-%m-%d %H:%M:%S")]

def ft_FPW_player(player, boxscore_data_cleaned):
    # Calcola i fantasy points utilizzando le statistiche disponibili e i coefficienti appropriati
    coeff_pts = 1.0
    coeff_reb = 1.2
    coeff_ast = 1.5
    coeff_stl = 3.0
    coeff_blk = 3.0
    coeff_to = -1.0
    
    player_stats=boxscore_data_cleaned[boxscore_data_cleaned["PLAYER_ID"] == player]

    fantasy_points = (player_stats['PTS'] * coeff_pts +
                    player_stats['REB'] * coeff_reb +
                    player_stats['AST'] * coeff_ast +
                    player_stats['STL'] * coeff_stl +
                    player_stats['BLK'] * coeff_blk +
                    player_stats['TO'] * coeff_to)
    # print(player)
    # print(fantasy_points)
    # print(player_stats['MIN'].to_list()[0])
    
    minutes = float(f"{player_stats['MIN'].to_list()[0].split('.')[0]}.{player_stats['MIN'].to_list()[0].split(':')[-1]}")
    #print(fantasy_points*minutes)
    ##pesato in base ai minuti giocati
    #print(f"minutes: {minutes}, fp: {float(fantasy_points)}")
    fantasy_points_w = float(fantasy_points.iloc[0]) * minutes
    #print("fw: ", fantasy_points_w)
    ## ritorno solo il FP dell'ultima season
    return fantasy_points_w

# Ottengo FP del team pesato
def get_FP_team_weigthed(team_players_id, boxscore_data_cleaned):
    fp_team = 0

    for player in team_players_id:
        fp_team += ft_FPW_player(player, boxscore_data_cleaned)
    return fp_team

def recent_team_strength_with_weights(team_stats, weights):
    avg_stats = np.mean(team_stats, axis=0)  # Calcola le medie delle statistiche recenti
    mms = MinMaxScaler()
    avg_stats = mms.fit_transform(pd.DataFrame(avg_stats)) * weights
    #normalized_avg_stats = normalize_stats_with_weights(avg_stats, weights)  # Normalizza le statistiche con pesi
    team_strength = np.mean(avg_stats)  # Calcola la media delle statistiche normalizzate come indice di forza relativa
    return team_strength

# Calcola la forza della squadra a partire dall'abbreviazione e enenendo conto solo delle ultime n_partite partite
def get_strength_by_abv(abv, weight, n_partite):
    # Ottieni i dati delle partite passate della squadra specificata
    game_log = teamgamelog.TeamGameLog(teams.find_team_by_abbreviation(abv)['id'])

    # Ottieni i dati dei DataFrame e tengo solo le ultime k partite
    game_log_data = game_log.get_data_frames()[0][:n_partite]

    #game_log_data = game_log_data[['PTS', 'FG_PCT', 'FT_PCT', 'OREB','DREB','AST','FG3_PCT','STL','BLK','W_PCT']]

    # CALCOLO FP PER OGNI PARTITA
    fp = []
    for index, row in game_log_data.iterrows():
        boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(row["Game_ID"])
 
        # Ottieni i dati dei DataFrame
        boxscore_data_cleaned = boxscore.get_data_frames()[0].dropna()

        team_players_id = boxscore_data_cleaned[boxscore_data_cleaned["TEAM_ABBREVIATION"] == row['MATCHUP'].split(" ")[0]]["PLAYER_ID"].to_list()
        fp.append(get_FP_team_weigthed(team_players_id, boxscore_data_cleaned))
    game_log_data['FP'] = fp

    # Peso il risultato anche in base alla percentuale di vittorie in questa stagione
    return recent_team_strength_with_weights(game_log_data[['PTS','FG_PCT', 'FT_PCT', 'OREB','DREB','AST','FG3_PCT','STL','BLK','FP']], weight) * game_log_data['W_PCT'][0]

def get_standings(conference):
    nba_league_standings = leaguestandings.LeagueStandings().get_data_frames()[0]
    standings = nba_league_standings[nba_league_standings['Conference'] == conference]
    df = pd.DataFrame(columns=['Position', 'TeamName', 'Wins', 'Losses', 'WinPCT', 'PointsPG'])
    for index, row in standings.iterrows():
        df.loc[df.shape[0]] = [df.shape[0] + 1, row['TeamCity'] + " " + row['TeamName'], + row['WINS'], + row['LOSSES'], int(row['WinPCT'] * 100), int(row['PointsPG'])]
    return df

def get_rank_players_blog():
    # Ottieni la classifica dei migliori giocatori
    leaders = leagueleaders.LeagueLeaders()
    return leaders.get_data_frames()[0][["PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "PTS", "MIN", "FGM", "FG_PCT"]][:2].to_json(orient="records")

def compute_starting_fp(player_stats):
    # Calcola i fantasy points utilizzando le statistiche disponibili e i coefficienti appropriati
    coeff_pts = 1.0
    coeff_reb = 1.2
    coeff_ast = 1.5
    coeff_stl = 3.0
    coeff_blk = 3.0
    coeff_to = -1.0

    fp = 0
    for index, row in player_stats.iterrows():
        fantasy_points = (row['PTS'] * coeff_pts +
                        row['REB'] * coeff_reb +
                        row['AST'] * coeff_ast +
                        row['STL'] * coeff_stl +
                        row['BLK'] * coeff_blk +
                        row['TO'] * coeff_to)
        fp = fp + fantasy_points
    return fp

def get_starting_strength(team_id):
    # Trova le partite recenti della squadra
    gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id)
    games = gamefinder.get_data_frames()[0]

    # Prendi l'ID della partita più recente
    recent_game_id = games.iloc[0]['GAME_ID']

    # Ottieni i dettagli della partita
    boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=recent_game_id)
    players_stats = boxscore.get_data_frames()[0]

    # Filtra i titolari
    starters = players_stats[players_stats['START_POSITION'].notnull()]

    # Visualizza i 5 titolari
    players_stats = starters[(starters['START_POSITION'] != '') & (starters['TEAM_ID'] == team_id)][['PTS','REB','AST','TO','STL','BLK']]

    return compute_starting_fp(players_stats)