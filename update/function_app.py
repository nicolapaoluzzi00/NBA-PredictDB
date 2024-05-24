import logging
import azure.functions as func
import pyodbc
import os

app = func.FunctionApp()

username = os.getenv('DBUsername')
password = os.getenv('DBpassword')
server = 'nbapredictdb.database.windows.net'
database = 'NBA-PredictDB'
driver = '{ODBC Driver 17 for SQL Server}'

@app.schedule(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=True) 
def UpdateStrenght(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password, autocommit=True)
        cursor = conn.cursor()
        query_ = "UPDATE PLAYERS SET name='X' WHERE id=2544"
        logging.info(query_)
        # Esecuzione della query
        cursor.execute(query_)

        # Chiudere il cursore e la connessione al database
        cursor.close()
        conn.close()
        #db.session.commit()
        # logging.info('The timer is past due!')

    logging.info('Fine')
'''import logging
import azure.functions as func

app = func.FunctionApp()

@app.schedule(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=True) 
def UpdateStrenght(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')'''