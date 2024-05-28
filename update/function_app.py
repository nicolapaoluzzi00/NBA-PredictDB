import logging
import azure.functions as func
import pyodbc
import os
from utility.functions import get_strength_by_abv
from nba_api.stats.endpoints import leaguegamelog, teamdetails

app = func.FunctionApp()

username = os.getenv('DBUsername')
password = os.getenv('DBpassword')
server = 'nbapredictdb.database.windows.net'
database = 'NBA-PredictDB'
driver = '{ODBC Driver 17 for SQL Server}'

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

@app.schedule(schedule="0 0 4 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=True) 
def UpdateStrenght(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password, autocommit=True)
        cursor = conn.cursor()
        query_ = "UPDATE PLAYERS SET name='X' WHERE id=2544"

        '''game_logs = leaguegamelog.LeagueGameLog(season = '2023-24')
        games_results = game_logs.get_data_frames()[0]
        id_list = games_results['TEAM_ID'].unique().tolist()
        for id in id_list:
            # Ottieni le informazioni dettagliate sulla squadra utilizzando l'endpoint 'teamdetails'
            team_info = teamdetails.TeamDetails(team_id=id).get_data_frames()[0]
            strength = get_strength_by_abv(str(team_info['ABBREVIATION'].iloc[0]), stats_weights, 30)
            query_ = f"UPDATE TEAMS SET strength={strength} WHERE id={id}"'''

        logging.info(query_)
        # Esecuzione della query
        cursor.execute(query_)

        # Chiudere il cursore e la connessione al database
        cursor.close()
        conn.close()

        # logging.info('The timer is past due!')

    logging.info('Fine')