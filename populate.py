from flask import Flask
from functions import get_schedule, get_strength_by_abv
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from tqdm import tqdm
from nba_api.stats.endpoints import leaguegamelog
from nba_api.stats.endpoints import teamdetails
from nba_api.stats.endpoints import leaguestandings, leagueleaders
import pyodbc

app = Flask(__name__)

username = 'NBA-Predict'
password = 'SRSProject2024'
server = 'nbapredictdb.database.windows.net'
database = 'NBA-PredictDB'
driver = 'ODBC Driver 18 for SQL Server'
app.config['SQLALCHEMY_DATABASE_URI'] = f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver={driver}'
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

class Player(db.Model):
    __tablename__ = 'PLAYERS'

    id = db.Column(db.Integer, primary_key = True)
    rank = db.Column(db.Integer)
    name = db.Column(db.String(30))
    team_id = db.Column(db.Integer)
    team_name = db.Column(db.String(50))
    pts = db.Column(db.Integer)
    min = db.Column(db.Integer)
    fgm = db.Column(db.Integer)
    fg_pct = db.Column(db.Float)

    def __init__(self, id, rank, name, team_id, team_name, pts, min, fgm, fg_pct):
        self.id = id
        self.rank = rank
        self.name = name
        self.team_id = team_id
        self.team_name = team_name
        self.pts = pts
        self.min = min
        self.fgm = fgm
        self.fg_pct = fg_pct

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

    # ------- GAMES --------
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

    # ------- TEAMS --------
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
                 strength = 1)
                 #strength = get_strength_by_abv(str(team_info['ABBREVIATION'].iloc[0]), stats_weights, 30))
        db.session.add(t)
    db.session.commit()

    # ------- PLAYERS --------
    leaders = leagueleaders.LeagueLeaders().get_data_frames()[0][["PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "PTS", "MIN", "FGM", "FG_PCT"]]
    for index, row in leaders.iterrows():
        p = Player(id = row['PLAYER_ID'],
                   rank = row['RANK'],
                   name = row['PLAYER'],
                   team_id = row['TEAM_ID'],
                   team_name = row['TEAM'],
                   pts = row['PTS'],
                   min = row['MIN'],
                   fgm = row['FGM'],
                   fg_pct = row['FG_PCT'])
        db.session.add(p)
        db.session.commit()

if __name__ == '__main__':
    with app.app_context():
        db.drop_all()
        populate_database()