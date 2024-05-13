from flask import Flask, render_template, request
from functions import get_schedule, get_first_official_by_game_id, get_strength_by_abv, get_future_schedule_2, get_rank_players, get_standings
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
import sqlite3
import pandas as pd
import os
from tqdm import tqdm
from nba_api.stats.endpoints import leaguegamelog
from nba_api.stats.endpoints import teamdetails
from nba_api.stats.endpoints import leaguestandings
import requests



from nba_api.stats.library.parameters import (
    LeagueID,
    Season,
    SeasonType,
    SeasonNullable,
)
from nba_api.stats.endpoints import playercareerstats
import pdb
from nba_api.stats.library.http import NBAStatsHTTP
from nba_api.stats.library.parameters import PerMode36, LeagueIDNullable

app = Flask(__name__)
BASEDIR = os.path.abspath(os.path.dirname(__name__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///'+os.path.join(BASEDIR,'NBAPredict')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

stats_weights = [0.2692530991195059,
                0.7209350734890745,
                0.19900967220334495,
                0.14953188233000456,
                0.8171771310389748,
                0.024835965923722118,
                0.30617936688553166,
                0.38573865209770575,
                0.18565061674123018,
                0.11693481303621481]

# Definire il modello di dati
class Games(db.Model):
    __tablename__ = 'GAMES'

    game_id = db.Column(db.String(10), primary_key=True)
    home_team_id = db.Column(db.Integer)
    home_team_tricode = db.Column(db.String(3))
    home_team_name = db.Column(db.String(50))
    away_team_id = db.Column(db.Integer)
    away_team_tricode = db.Column(db.String(3))
    away_team_name = db.Column(db.String(50))
    start_time = db.Column(db.DateTime)
    home_pts = db.Column(db.Integer)
    away_pts = db.Column(db.Integer)

    def __init__(self, game_id, home_team_id, home_team_tricode, home_team_name, away_team_id, away_team_tricode, away_team_name, start_time, home_pts, away_pts):
        self.game_id = game_id
        self.home_team_id = home_team_id
        self.home_team_name = home_team_name
        self.home_team_tricode = home_team_tricode
        self.away_team_id = away_team_id
        self.away_team_name = away_team_name
        self.away_team_tricode = away_team_tricode
        self.start_time = start_time
        self.away_pts = away_pts
        self.home_pts = home_pts

class Team(db.Model):
    __tablename__ = 'TEAMS'

    id = db.Column(db.Integer, primary_key = True)
    abv = db.Column(db.String(3))
    name = db.Column(db.String(50))
    strength = db.Column(db.Float)
    conference = db.Column(db.String(5))
    position = db.Column(db.Integer)
    wins = db.Column(db.Integer)
    losses = db.Column(db.Integer)
    win_pct = db.Column(db.Float)
    ppg = db.Column(db.Integer)

    def __init__(self, team_id, team_abv, team_name, strength, conference, position, wins, losses, win_pct, ppg):
        self.id = team_id
        self.abv = team_abv
        self.name = team_name
        self.strength = strength
        self.conference = conference
        self.position = position
        self.wins = wins
        self.losses = losses
        self.win_pct = win_pct
        self.ppg = ppg

def return_next_match(home_team_id, away_team_id, next_matches):
    conn = sqlite3.connect('NBAPredict')
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
    if len(rows) > 0:
        # prendo solo la prima riga perchè mi interessa solo il prossimo match
        next_m = list(rows[0])
        print(next_m)
        game_label = next_matches[next_matches["game_id"] == rows[0][0]]["game_label"].iloc[0]
        arena_name = next_matches[next_matches["game_id"] == rows[0][0]]["arena_name"].iloc[0]
        arena_city = next_matches[next_matches["game_id"] == rows[0][0]]["arena_city"].iloc[0]

        next_m.append(game_label)
        next_m.append(arena_name)
        next_m.append(arena_city)

        df = pd.DataFrame([next_m], columns = ['game_id', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'start_time', 'game_label', 'arena_name', 'arena_city'])

    df = pd.DataFrame(columns = ['game_id', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'start_time', 'game_label', 'arena_name', 'arena_city'])

    return df.to_json(orient="records")

def return_upcoming_match(team_id, next_matches):
    conn = sqlite3.connect('NBAPredict')
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

    # Chiudere il cursore e la connessione al database
    cursor.close()
    conn.close()

    return df.to_json(orient="records")

# Funzione per popolare il database con dati predefiniti
def populate_database():
    # Crea tutte le tabelle nel database
    db.create_all()

    # Popola il database con dati predefiniti
    games = get_schedule()

    # Sistemo la data d'inizio
    new_start = games['datetime'].str.replace('Z','')
    new_start = new_start.str.replace('T','-')
    new_start = new_start.str.replace(':','-')
    games['datetime'] = new_start

    game_logs = leaguegamelog.LeagueGameLog(season = '2023-24')
    games_results = game_logs.get_data_frames()[0]

    # Popolo tabella delle partite giocate
    for index, row in games.iterrows():
        date = row['datetime'].split('-')
        custom_datetime = datetime(int(date[0]), int(date[1]), int(date[2]), int(date[3]), int(date[4]))  # Anno, mese, giorno, ora, minuto
        if custom_datetime < datetime(2024,3,1):
            home_pts = int(games_results[(games_results['GAME_ID'] == row['game_id']) & (games_results['TEAM_ABBREVIATION'] == row['home_team_tricode'])]['PTS'].iloc[0])
            away_pts = int(games_results[(games_results['GAME_ID'] == row['game_id']) & (games_results['TEAM_ABBREVIATION'] == row['away_team_tricode'])]['PTS'].iloc[0])
        else:
            home_pts = None
            away_pts = None
        g = Games(game_id = row['game_id'],
                    home_team_id = row['home_team_id'],
                    home_team_tricode = row['home_team_tricode'],
                    home_team_name = row['home_team_city'] + ' ' + row['home_team_name'],
                    away_team_id = row['away_team_id'],
                    away_team_tricode = row['away_team_tricode'],
                    away_team_name =  row['away_team_city'] + ' ' + row['away_team_name'],
                    start_time = custom_datetime,
                    home_pts = home_pts,
                    away_pts = away_pts)

        db.session.add(g)
    
    # Esegui il commit delle modifiche
    db.session.commit()

    # Popolo tabella delle squadre
    id_list = games_results['TEAM_ID'].unique().tolist()
    for id in tqdm(id_list):
        # Ottieni le informazioni dettagliate sulla squadra utilizzando l'endpoint 'teamdetails'
        team_info = teamdetails.TeamDetails(team_id=id).get_data_frames()[0]
        nba_league_standings = leaguestandings.LeagueStandings(timeout=100).get_data_frames()[0]
        nba_league_standings = nba_league_standings[nba_league_standings['TeamID'] == id]
        t = Team(team_id = id,
                 team_abv = str(team_info['ABBREVIATION'].iloc[0]),
                 team_name = str(team_info['CITY'].iloc[0]) + ' ' + str(team_info['NICKNAME'].iloc[0]),
                 conference = nba_league_standings['Conference'].iloc[0],
                 position = int(nba_league_standings['PlayoffRank'].iloc[0]),
                 wins = int(nba_league_standings['WINS'].iloc[0]),
                 losses = int(nba_league_standings['LOSSES'].iloc[0]),
                 win_pct = int(nba_league_standings['WinPCT'].iloc[0] * 100),
                 ppg = int(nba_league_standings['PointsPG'].iloc[0]),
                 strength = 0)
                 #strength = get_strength_by_abv(str(team_info['ABBREVIATION'].iloc[0]), stats_weights, 30))
        db.session.add(t)
    db.session.commit()

@app.route("/test")
def test():
    # game_logs = leaguegamelog.LeagueGameLog(season = '2023-24')
    # Nikola Jokić
    # career = playercareerstats.PlayerCareerStats(player_id='203999') 
    response = requests.get(
        url="https://stats.nba.com/players",
        params={
            "LeagueID": LeagueID.default,
            "Season": Season.default,
            "SeasonType": SeasonType.default,
            "SeasonYear": SeasonNullable.default,
        },
        headers=None,
        proxies=None,
        timeout=30,
    )
    print(response.status_code)
    return render_template('test.html')

@app.route("/")
def homepage():
    conn = sqlite3.connect('NBAPredict')
    cursor = conn.cursor()

    # Esecuzione di una query SQL
    cursor.execute("SELECT game_id, home_team_id, home_team_name, away_team_id, away_team_name, start_time, home_pts, away_pts FROM GAMES")
    # Ottenere i risultati della query
    rows = cursor.fetchall()

    cursor.execute("SELECT position, name, wins, losses, win_pct, ppg FROM TEAMS WHERE conference = 'East' ORDER BY position")
    eastStandings = pd.DataFrame(cursor.fetchall(), columns=['Position', 'TeamName', 'Wins', 'Losses', 'WinPCT', 'PointsPG']).to_json(orient="records")

    cursor.execute("SELECT position, name, wins, losses, win_pct, ppg FROM TEAMS WHERE conference = 'West' ORDER BY position")
    westStandings = pd.DataFrame(cursor.fetchall(), columns=['Position', 'TeamName', 'Wins', 'Losses', 'WinPCT', 'PointsPG']).to_json(orient="records")

    # Chiudere il cursore e la connessione al database
    cursor.close()
    conn.close()

    df = pd.DataFrame(rows, columns = ['game_id', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'start_time', 'home_pts', 'away_pts'])

    return render_template('index.html', partite = df.to_json(orient="records"),
                           eastStandings = eastStandings,
                           westStandings = westStandings)
                           #rank_players = get_rank_players())

@app.route("/game_details")
def game_details():
    game_id = request.args.get('game_id', '')
    first_official = get_first_official_by_game_id(game_id)
    game_logs = leaguegamelog.LeagueGameLog(season = '2023-24')
    games = game_logs.get_data_frames()[0]
    home_stats = games[(games['GAME_ID'] == game_id) & (games['MATCHUP'].str.contains('vs.'))]
    game_date = home_stats['GAME_DATE'].iloc[0]
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
    next_matches = get_future_schedule_2()
    home_tid = home_stats['TEAM_ID'].iloc[0]
    away_tid = away_stats['TEAM_ID'].iloc[0]
    # Upcoming match home
    upcoming_matches_H = return_upcoming_match(home_tid, next_matches)
    # Upcoming match away
    upcoming_matches_A = return_upcoming_match(away_tid, next_matches)
    next = return_next_match(home_tid, away_tid, next_matches)
    date = game_date.split('-')
    custom_datetime = datetime(int(date[0]), int(date[1]), int(date[2]))
    if custom_datetime < datetime(2024, 3, 1):
        return render_template('matches.html',
                            first_official=first_official,
                            home_stats=home_stats.to_json(orient="records"),
                            away_stats=away_stats.to_json(orient="records"),
                            next_match = next,
                            upcoming_matches_H = upcoming_matches_H,
                            upcoming_matches_A = upcoming_matches_A,
                            rank_players = get_rank_players())

    # Predno le forze
    conn = sqlite3.connect('NBAPredict')
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
                            rank_players = get_rank_players(),
                            home_win_pct = home_win_pct,
                            away_win_pct = away_win_pct)

if __name__ == '__main__':
    with app.app_context():
        # db.drop_all()

        # Connessione al database
        conn = sqlite3.connect('NBAPredict')
        cursor = conn.cursor()

        # Esegui una query per ottenere l'elenco delle tabelle
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tabelle = cursor.fetchall()

        # Chiudi la connessione
        conn.close()

        # Conta il numero di tabelle
        numero_tabelle = len(tabelle)

        # Verifica se il numero di tabelle è zero
        if numero_tabelle == 0:
            populate_database()
    '''response = requests.get(url="https://stats.nba.com/stats/playercareerstats",
                            params={
                                "PlayerID": '203999',
                                "PerMode": PerMode36.default,
                                "LeagueID": LeagueIDNullable.default,
                            },
                            headers={
                                #"Host": "stats.nba.com",
                                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0",
                                "Referer": "https://stats.nba.com/"
                            },
                            timeout=30)

    response = requests.get(
            url="https://stats.nba.com/stats/playercareerstats",
            params={
                "PlayerID": '203999',
                "PerMode": PerMode36.default,
                "LeagueID": LeagueIDNullable.default,
            },
            proxies=None,
            headers={
                "Host": "stats.nba.com",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "x-nba-stats-origin": "stats",
                "x-nba-stats-token": "true",
                "Connection": "keep-alive",
                "Referer": "https://stats.nba.com/",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
            },
            timeout=30,
        )

    career = playercareerstats.PlayerCareerStats(player_id='203999') '''
    app.run(debug=True)