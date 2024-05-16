from flask import Flask, render_template, request
from functions import get_schedule, get_first_official_by_game_id, get_strength_by_abv, get_future_schedule_2, get_rank_players, get_standings, get_rank_players_blog
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
from nba_api.stats.endpoints import leaguegamelog
from nba_api.stats.endpoints import teamdetails
from nba_api.stats.endpoints import leaguestandings, leagueleaders
import requests
import pyodbc
from time import time
import warnings
warnings.filterwarnings('ignore')

app = Flask(__name__)
# BASEDIR = os.path.abspath(os.path.dirname(__name__))
# app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///'+os.path.join(BASEDIR,'NBAPredict')
username = 'NBA-Predict'
password = 'SRSProject2024'
server = 'nbapredictdb.database.windows.net'
database = 'NBA-PredictDB'
driver = 'ODBC Driver 18 for SQL Server'
app.config['SQLALCHEMY_DATABASE_URI'] = f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver={driver}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

def return_next_match(home_team_id, away_team_id, next_matches):
    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    query_ = f'''
        SELECT game_id, home_team_id, home_team_name, away_team_id, away_team_name, start_time
        FROM GAMES
        WHERE ((home_team_id = {home_team_id} AND away_team_id = {away_team_id}) OR (home_team_id = {away_team_id} AND away_team_id = {home_team_id})) AND home_pts is null
    '''
    # Esecuzione della query
    cursor.execute(query_)

    # Ottenere i risultati della query
    rows = cursor.fetchall()

    # Chiudere il cursore e la connessione al database
    cursor.close()
    conn.close()
    next_m = []
    df = pd.DataFrame(columns = ['game_id', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'start_time', 'game_label', 'arena_name', 'arena_city'])

    if len(rows) > 0:
        # prendo solo la prima riga perchè mi interessa solo il prossimo match
        next_m = list(rows[0])
        # print(next_m)
        game_label = next_matches[next_matches["game_id"] == rows[0][0]]["game_label"].iloc[0]
        arena_name = next_matches[next_matches["game_id"] == rows[0][0]]["arena_name"].iloc[0]
        arena_city = next_matches[next_matches["game_id"] == rows[0][0]]["arena_city"].iloc[0]

        next_m.append(game_label)
        next_m.append(arena_name)
        next_m.append(arena_city)

        df = pd.DataFrame([next_m], columns = ['game_id', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'start_time', 'game_label', 'arena_name', 'arena_city'])

    df['start_time'] = df['start_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    return df.to_json(orient="records")

def return_upcoming_match(team_id, next_matches):
    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    query_ = f'''
        SELECT game_id, home_team_id, home_team_name, away_team_id, away_team_name, start_time
        FROM GAMES
        WHERE (home_team_id = {team_id} OR away_team_id = {team_id}) AND home_pts is null
    '''
    # Esecuzione della query
    cursor.execute(query_)

    # Ottenere i risultati della query
    rows = cursor.fetchall()

    # prendo solo la prima riga perchè mi interessa solo il prossimo match
    next_m = list(rows[:2])

    upcoming_m = []
    for row in next_m:
        game_label = next_matches[next_matches["game_id"] == row[0]]["game_label"].values[0]
        arena_name = next_matches[next_matches["game_id"] == row[0]]["arena_name"].values[0]
        arena_city = next_matches[next_matches["game_id"] == row[0]]["arena_city"].values[0]
        row = list(row)
        row.append(game_label)
        row.append(arena_name)
        row.append(arena_city)

        upcoming_m.append(row)

    df = pd.DataFrame(upcoming_m, columns = ['game_id', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'start_time', 'game_label', 'arena_name', 'arena_city'])

    df['start_time'] = df['start_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    
    # Chiudere il cursore e la connessione al database
    cursor.close()
    conn.close()

    return df.to_json(orient="records")

@app.route("/test")
def test():
    # # game_logs = leaguegamelog.LeagueGameLog(season = '2023-24')
    # # Nikola Jokić
    # # career = playercareerstats.PlayerCareerStats(player_id='203999') 
    # response = requests.get(url="https://stats.nba.com/players")
    # print(response.status_code)
    # return render_template('test.html')
    import os
    os.system("python -m streamlit prova.py --server.port 8000 --server.address 0.0.0.0")

@app.route("/")
def homepage():
    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()

    # Esecuzione di una query SQL
    df = pd.read_sql("SELECT game_id, home_team_id, home_team_name, away_team_id, away_team_name, start_time, home_pts, away_pts FROM GAMES", conn)
    df.columns = ['game_id', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'start_time', 'home_pts', 'away_pts']
    
    # Classifiche
    eastStandings = pd.read_sql("SELECT position, name, wins, losses, win_pct, ppg FROM TEAMS WHERE conference = 'East' ORDER BY position", conn)
    eastStandings.columns=['Position', 'TeamName', 'Wins', 'Losses', 'WinPCT', 'PointsPG']
    eastStandings = eastStandings.to_json(orient="records")

    westStandings = pd.read_sql("SELECT position, name, wins, losses, win_pct, ppg FROM TEAMS WHERE conference = 'West' ORDER BY position", conn)
    westStandings.columns=['Position', 'TeamName', 'Wins', 'Losses', 'WinPCT', 'PointsPG']
    westStandings = westStandings.to_json(orient="records")

    # Top players
    rank_players = pd.read_sql("SELECT id, rank, name, team_id, team_name, pts FROM PLAYERS ORDER BY rank", conn)[:10]
    rank_players.columns=["PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "PTS"]
    rank_players = rank_players.to_json(orient="records")

    # Player blog
    rank_players_blog = pd.read_sql("SELECT id, rank, name, team_id, team_name, pts, min, fgm, fg_pct FROM PLAYERS ORDER BY rank", conn)[:2]
    rank_players_blog.columns=["PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "PTS", "MIN", "FGM", "FG_PCT"]
    rank_players_blog = rank_players_blog.to_json(orient="records")
    # Chiudere il cursore e la connessione al database
    cursor.close()
    conn.close()
    df['start_time'] = df['start_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    return render_template('index.html', partite = df.to_json(orient="records"),
                           eastStandings = eastStandings,
                           westStandings = westStandings,
                           rank_players = rank_players,
                           rank_players_blog = rank_players_blog)

@app.route("/game_details")
def game_details():
    game_id = request.args.get('game_id', '')
    # first_official = get_first_official_by_game_id(game_id)

    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    
    t0 = time()
    # Top players
    rank_players = pd.read_sql("SELECT id, rank, name, team_id, team_name, pts FROM PLAYERS ORDER BY rank", conn)[:10]
    rank_players.columns=["PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "PTS"]
    rank_players = rank_players.to_json(orient="records")
    print(f'PLAYER = {time()-t0}')

    t0 = time()
    # Player blog
    rank_players_blog = pd.read_sql("SELECT id, rank, name, team_id, team_name, pts, min, fgm, fg_pct FROM PLAYERS ORDER BY rank", conn)[:2]
    rank_players_blog.columns=["PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "PTS", "MIN", "FGM", "FG_PCT"]
    rank_players_blog = rank_players_blog.to_json(orient="records")
    print(f'BLOG = {time()-t0}')

    t0 = time()
    games = pd.read_sql(f"SELECT * FROM GAMESLOG WHERE GAME_ID = {game_id}", conn)
    games.columns = ['ID', 'TEAM_ID', 'GAME_ID', 'TEAM_NAME', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'MATCHUP', 'GAME_DATE', 'REF']
    print(f'GAMES = {time()-t0}')



    t0 = time()
    next_matches = pd.read_sql("SELECT game_id, home_team_id, home_team_tricode, home_team_name, away_team_id, away_team_tricode, away_team_name, start_time, game_label, arena_name, arena_city FROM GAMES WHERE start_time >= '2024-03-01'", conn)
    next_matches.columns=['game_id', 'home_team_id', 'home_team_tricode', 'home_team_name', 'away_team_id', 'away_team_tricode', 'away_team_name', 'datetime', 'game_label', 'arena_name', 'arena_city']
    print(f'FUTURE SCHEDULE DB = {time()-t0}')

    # Chiudere la connessione al database
    conn.close()

    first_official = games['REF'].iloc[0]
    home_stats = games[(games['GAME_ID'] == game_id) & (games['MATCHUP'].str.contains('vs.'))]
    game_date = home_stats['GAME_DATE'].iloc[0].strftime('%Y-%m-%d')
    # df['start_time'] = df['start_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    home_stats = home_stats[['TEAM_ID','TEAM_NAME', 'FGM', 'FGA', 'FG_PCT', 'FG3M',
                            'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'AST',
                            'STL', 'BLK', 'TOV', 'PF', 'PTS']]
    home_stats['FG_PCT'] = int(round(home_stats['FG_PCT'].iloc[0],2)*100)
    home_stats['FG3_PCT'] = int(round(home_stats['FG3_PCT'].iloc[0],2)*100)
    home_stats['FT_PCT'] = int(round(home_stats['FT_PCT'].iloc[0],2)*100)
    
    away_stats = games[(games['GAME_ID'] == game_id) & (games['MATCHUP'].str.contains('@'))]
    away_stats = away_stats[['TEAM_ID','TEAM_NAME', 'FGM', 'FGA', 'FG_PCT', 'FG3M',
                            'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'AST',
                            'STL', 'BLK', 'TOV', 'PF', 'PTS']]
    away_stats['FG_PCT'] = int(round(away_stats['FG_PCT'].iloc[0],2)*100)
    away_stats['FG3_PCT'] = int(round(away_stats['FG3_PCT'].iloc[0],2)*100)
    away_stats['FT_PCT'] = int(round(away_stats['FT_PCT'].iloc[0],2)*100)
    
    t0 = time()
    # next_matches = get_future_schedule_2()
    print(f'FUTURE SCHEDULE = {time()-t0}')
    home_tid = home_stats['TEAM_ID'].iloc[0]
    away_tid = away_stats['TEAM_ID'].iloc[0]

    t0 = time()
    # Upcoming match home
    upcoming_matches_H = return_upcoming_match(home_tid, next_matches)
    # Upcoming match away
    upcoming_matches_A = return_upcoming_match(away_tid, next_matches)
    next = return_next_match(home_tid, away_tid, next_matches)
    date = game_date.split('-')
    custom_datetime = datetime(int(date[0]), int(date[1]), int(date[2]))
    print(f'NEXT MATCHES = {time()-t0}')

    if custom_datetime < datetime(2024, 3, 1):
        return render_template('matches.html',
                            first_official=first_official,
                            home_stats=home_stats.to_json(orient="records"),
                            away_stats=away_stats.to_json(orient="records"),
                            next_match = next,
                            upcoming_matches_H = upcoming_matches_H,
                            upcoming_matches_A = upcoming_matches_A,
                            rank_players = rank_players,
                            rank_players_blog = rank_players_blog)

    # Predno le forze
    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    # Esecuzione di una query SQL
    cursor.execute(f"SELECT strength FROM TEAMS WHERE id = {home_tid}")
    # Ottenere i risultati della query
    home_strength = cursor.fetchall()[0][0]

    # Esecuzione di una query SQL
    cursor.execute(f"SELECT strength FROM TEAMS WHERE id = {away_tid}")
    # Ottenere i risultati della query
    away_strength = cursor.fetchall()[0][0]
    
    # Chiudere il cursore e la connessione al database
    cursor.close()
    conn.close()

    # Calcolo probabilità di vittoria
    home_win_pct = round(home_strength/(home_strength + away_strength)*100, 2)
    away_win_pct = round(away_strength/(home_strength + away_strength)*100, 2)

    return render_template('matches_prediction.html',
                            home_stats=home_stats.to_json(orient="records"),
                            away_stats=away_stats.to_json(orient="records"),
                            first_official=first_official,
                            next_match = next,
                            upcoming_matches_H = upcoming_matches_H,
                            upcoming_matches_A = upcoming_matches_A,
                            rank_players = rank_players,
                            home_win_pct = home_win_pct,
                            away_win_pct = away_win_pct,
                            rank_players_blog = rank_players_blog)

if __name__ == '__main__':
    app.run(debug=True)