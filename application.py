from flask import Flask, render_template, request, stream_template, get_template_attribute, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import pandas as pd
import os
import pyodbc
from time import time
import warnings
warnings.filterwarnings('ignore')
import pdb
from utility import query as q 
from utility.functions import run_chatbot

app = Flask(__name__)

## database locale
# BASEDIR = os.path.abspath(os.path.dirname(__name__))
# app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///'+os.path.join(BASEDIR,'NBAPredict')

##serve in locale
# username = 'NBA-Predict'
# password = 'SRSProject2024'
username = os.getenv('DBUsername')
password = os.getenv('DBpassword')
server = 'nbapredictdb.database.windows.net'
database = 'NBA-PredictDB'
driver = 'ODBC Driver 18 for SQL Server'
app.config['SQLALCHEMY_DATABASE_URI'] = f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver={driver}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

################### CHATBOT ###################
# from langchain_community.document_loaders.pdf import PyPDFLoader
# from langchain.text_splitter import RecursiveCharacterTextSplitter
# from langchain.embeddings import HuggingFaceEmbeddings
# from langchain.vectorstores import Chroma, FAISS
# from langchain.chains import RetrievalQA
# from langchain_community.llms import HuggingFaceEndpoint
################### CHATBOT ###################

## serve in locale
# from dotenv import load_dotenv
# load_dotenv("./.env")

# pdb.set_trace()
# huggingface_hub = os.getenv('HUGGINGFACEHUB_API_TOKEN')
# print(huggingface_hub)

# path = "./data/games.pdf"

# # Loader
# loader = PyPDFLoader(path)
# data = loader.load()

# print("load fatto")

# #Document Transformers
# # Create an instance of the RecursiveCharacterTextSplitter class with specific parameters.
# # It splits text into chunks of 1000 characters each with a 150-character overlap.
# text_splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=100)
# # 'data' holds the text you want to split, split the text into documents using the text splitter.
# docs = text_splitter.split_documents(data)

# print("split fatto")

# #EMBEDDINGS
# # Define the path to the pre-trained model you want to use
# modelPath = "sentence-transformers/all-MiniLM-l6-v2"

# # Create a dictionary with model configuration options, specifying to use the CPU for computations
# model_kwargs = {'device':'cpu'}

# # Create a dictionary with encoding options, specifically setting 'normalize_embeddings' to False
# encode_kwargs = {'normalize_embeddings': False}

# # Initialize an instance of HuggingFaceEmbeddings with the specified parameters
# embeddings = HuggingFaceEmbeddings(
#     model_name=modelPath,     # Provide the pre-trained model's path
#     model_kwargs=model_kwargs, # Pass the model configuration options
#     encode_kwargs=encode_kwargs # Pass the encoding options
# )

# print("modello di embedd fatto")

# db = FAISS.from_documents(docs, embeddings)

# print("vector stores fatto")

# # Create a retriever object from the 'db' with a search configuration where it retrieves up to 4 relevant splits/documents.
# retriever = db.as_retriever(search_kwargs={"k": 4})

# print("retriever fatto")

# llm=HuggingFaceEndpoint(repo_id="mistralai/Mistral-7B-Instruct-v0.2", temperature=0.1, max_length=512)

# print("llm fatto")

# chain = RetrievalQA.from_chain_type(llm=llm,
#                                     chain_type="stuff",
#                                     retriever=retriever,
#                                     input_key="question")

# print("chain fatto")

db = SQLAlchemy(app)

def return_next_match(home_team_id, away_team_id, next_matches):
    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    
    # Esecuzione della query
    cursor.execute(q.query1,[str(home_team_id), str(away_team_id), str(away_team_id), str(home_team_id)])

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
    
    # Esecuzione della query
    cursor.execute(q.query2, [str(team_id), str(team_id)])

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

# @app.route('/data', methods=['GET'])
# def get_data():
    # run_chatbot(request=request, chain=chain)

@app.route("/")
def homepage():
    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()

    # Esecuzione di una query SQL
    df = pd.read_sql(q.query3, conn)
    df.columns = ['game_id', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'start_time', 'home_pts', 'away_pts']
    
    # Classifiche
    eastStandings = pd.read_sql(q.query4, conn)
    eastStandings.columns=['Position', 'TeamName', 'Wins', 'Losses', 'WinPCT', 'PointsPG']
    eastStandings = eastStandings.to_json(orient="records")

    westStandings = pd.read_sql(q.query5, conn)
    westStandings.columns=['Position', 'TeamName', 'Wins', 'Losses', 'WinPCT', 'PointsPG']
    westStandings = westStandings.to_json(orient="records")

    # Top players
    rank_players = pd.read_sql(q.query6, conn)[:10]
    rank_players.columns=["PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "PTS"]
    rank_players = rank_players.to_json(orient="records")

    # Player blog
    rank_players_blog = pd.read_sql(q.query7, conn)[:2]
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

    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)

    # Top players
    rank_players = pd.read_sql(q.query6, conn)[:10]
    rank_players.columns=["PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "PTS"]
    rank_players = rank_players.to_json(orient="records")

    # Player blog
    rank_players_blog = pd.read_sql(q.query7, conn)[:2]
    rank_players_blog.columns=["PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "PTS", "MIN", "FGM", "FG_PCT"]
    rank_players_blog = rank_players_blog.to_json(orient="records")
    
    games = pd.read_sql(q.query10, conn, params=[str(game_id)])
    games.columns = ['ID', 'TEAM_ID', 'GAME_ID', 'TEAM_NAME', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'MATCHUP', 'GAME_DATE', 'REF']

    next_matches = pd.read_sql(q.query11, conn)
    next_matches.columns=['game_id', 'home_team_id', 'home_team_tricode', 'home_team_name', 'away_team_id', 'away_team_tricode', 'away_team_name', 'datetime', 'game_label', 'arena_name', 'arena_city']
    
    # Chiudere la connessione al database
    conn.close()

    first_official = games['REF'].iloc[0]
    home_stats = games[(games['GAME_ID'] == game_id) & (games['MATCHUP'].str.contains('vs.'))]
    game_date = home_stats['GAME_DATE'].iloc[0].strftime('%Y-%m-%d')
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

    # Prendo le forze
    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    # Esecuzione di una query SQL
    cursor.execute(q.query12, [str(home_tid)])
    # Ottenere i risultati della query
    home_starting_strength = cursor.fetchall()[0][0]

    # Esecuzione di una query SQL
    cursor.execute(q.query12, [str(away_tid)])
    # Ottenere i risultati della query
    away_starting_strength = cursor.fetchall()[0][0]
    
    # Chiudere il cursore e la connessione al database
    cursor.close()
    conn.close()

    if custom_datetime < datetime(2024, 3, 1):
        return render_template('matches.html',
                            first_official=first_official,
                            home_stats=home_stats.to_json(orient="records"),
                            away_stats=away_stats.to_json(orient="records"),
                            next_match = next,
                            upcoming_matches_H = upcoming_matches_H,
                            upcoming_matches_A = upcoming_matches_A,
                            rank_players = rank_players,
                            rank_players_blog = rank_players_blog,
                            home_starting_strength = home_starting_strength,
                            away_starting_strength = away_starting_strength)

    # Prendo le forze
    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    # Esecuzione di una query SQL
    cursor.execute(q.query13, [str(home_tid)])
    # Ottenere i risultati della query
    home_strength = cursor.fetchall()[0][0]

    # Esecuzione di una query SQL
    cursor.execute(q.query13, [str(away_tid)])
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
                            rank_players_blog = rank_players_blog,
                            home_starting_strength = home_starting_strength,
                            away_starting_strength = away_starting_strength)

if __name__ == '__main__':
    app.run(debug=False)