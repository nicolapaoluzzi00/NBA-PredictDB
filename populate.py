from flask import Flask
from functions import get_schedule, get_strength_by_abv
from flask_sqlalchemy import SQLAlchemy, model
from datetime import datetime
from tqdm import tqdm
from nba_api.stats.endpoints import leaguegamelog
from nba_api.stats.endpoints import teamdetails, boxscoresummaryv2
from nba_api.stats.endpoints import leaguestandings, leagueleaders
import pyodbc
import math

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
    game_label = db.Column(db.String(15))
    arena_name = db.Column(db.String(50))
    arena_city = db.Column(db.String(50))

    def __init__(self, game_id, home_team_id, home_team_tricode, home_team_name, away_team_id, away_team_tricode, away_team_name, start_time, home_pts, away_pts, game_label, arena_name, arena_city):
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
        self.arena_name = arena_name
        self.arena_city = arena_city
        self.game_label = game_label

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

class GameLog(db.Model):
    __tablename__ = 'GAMESLOG'

    id = db.Column(db.Integer, primary_key = True)
    team_id = db.Column(db.Integer)
    game_id = db.Column(db.String(10))
    team_name = db.Column(db.String(50))
    fgm = db.Column(db.Integer)
    fga = db.Column(db.Integer)
    fg_pct = db.Column(db.Float)
    fg3m = db.Column(db.Integer)
    fg3a = db.Column(db.Integer)
    fg3_pct = db.Column(db.Float)
    ftm = db.Column(db.Integer)
    fta = db.Column(db.Integer)
    ft_pct = db.Column(db.Float)
    oreb = db.Column(db.Integer)
    dreb = db.Column(db.Integer)
    ast = db.Column(db.Integer)
    stl = db.Column(db.Integer)
    blk = db.Column(db.Integer)
    tov = db.Column(db.Integer)
    pf = db.Column(db.Integer)
    pts = db.Column(db.Integer)
    matchup = db.Column(db.String(15))
    game_date = db.Column(db.DateTime)
    ref = db.Column(db.String(50))

    def __init__(self, id, team_id, game_id, team_name, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, ast, stl, blk, tov, pf, pts, matchup, game_date, ref):
        self.id = id
        self.team_id = team_id
        self.game_id = game_id
        self.team_name = team_name
        self.fgm = fgm
        self.fga = fga
        self.fg_pct = fg_pct
        self.fg3m = fg3m
        self.fg3a = fg3a
        self.fg3_pct = fg3_pct
        self.ftm = ftm
        self.fta = fta
        self.ft_pct = ft_pct
        self.oreb = oreb
        self.dreb = dreb
        self.ast = ast
        self.stl = stl
        self.blk = blk
        self.tov = tov
        self.pf = pf
        self.pts = pts
        self.matchup = matchup
        self.game_date = game_date
        self.ref = ref

def populate_games():
    Games.query.delete()
    db.session.commit()
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
                    away_pts = away_pts,
                    game_label = row['game_label'],
                    arena_city = row['arena_city'],
                    arena_name = row['arena_name'])

        db.session.add(g)
    
    # Esegui il commit delle modifiche
    db.session.commit()

def populate_gameslog():
    GameLog.query.delete()
    db.session.commit()
    # -------- GAMESLOG --------
    print('GAMES LOG')
    game_logs = leaguegamelog.LeagueGameLog(season = '2023-24').get_data_frames()[0]
    # game_logs[game_logs['GAME_ID'] == '0022301148']['FT_PCT'].iloc[1] = 0
    for index, row in tqdm(game_logs.iterrows()):
        if math.isnan(row['FG_PCT']):
            row['FG_PCT'] = 0
        if math.isnan(row['FG3_PCT']):
            row['FG3_PCT'] = 0
        if math.isnan(row['FT_PCT']):
            row['FT_PCT'] = 0

        # Find referee
        game_id = row['GAME_ID']
        boxscore = boxscoresummaryv2.BoxScoreSummaryV2(game_id=game_id)
        data = boxscore.get_normalized_dict()['Officials'][0]
        first_official = data['FIRST_NAME'] + " " + data['LAST_NAME']
        gl = GameLog(id = index,
                     team_id = row['TEAM_ID'],
                     team_name = row['TEAM_NAME'],
                     game_id = game_id,
                     fgm = row['FGM'],
                     fga = row['FGA'],
                     fg_pct = row['FG_PCT'],
                     fg3m = row['FG3M'],
                     fg3a = row['FG3A'],
                     fg3_pct = row['FG3_PCT'],
                     ftm = row['FTM'],
                     fta = row['FTA'],
                     ft_pct = row['FT_PCT'],
                     oreb = row['OREB'],
                     dreb = row['OREB'],
                     ast = row['AST'],
                     stl = row['STL'],
                     blk = row['BLK'],
                     tov = row['TOV'],
                     pf = row['PF'],
                     pts = row['PTS'],
                     matchup = row['MATCHUP'],
                     game_date=row['GAME_DATE'],
                     ref = first_official)
        db.session.add(gl)
        db.session.commit()

def populate_teams():
    Team.query.delete()
    db.session.commit()
# ------- TEAMS --------
    game_logs = leaguegamelog.LeagueGameLog(season = '2023-24')
    games_results = game_logs.get_data_frames()[0]
    id_list = games_results['TEAM_ID'].unique().tolist()
    print('TEAMS')
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

def populate_players():
    Player.query.delete()
    db.session.commit()
    # ------- PLAYERS --------
    leaders = leagueleaders.LeagueLeaders().get_data_frames()[0][["PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "PTS", "MIN", "FGM", "FG_PCT"]]
    print('PLAYERS')
    for index, row in tqdm(leaders.iterrows()):
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
        db.create_all()
        # populate_teams()
        # populate_players()
        populate_games()        
        # populate_gameslog()