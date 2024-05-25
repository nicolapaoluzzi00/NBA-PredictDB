query1 = f'''
        SELECT game_id, home_team_id, home_team_name, away_team_id, away_team_name, start_time
        FROM GAMES
        WHERE ((home_team_id = ? AND away_team_id = ?) OR (home_team_id = ? AND away_team_id = ?)) AND home_pts is null
    '''

query2 = f'''
        SELECT game_id, home_team_id, home_team_name, away_team_id, away_team_name, start_time
        FROM GAMES
        WHERE (home_team_id = ? OR away_team_id = ?) AND home_pts is null
    '''
    
query3 = "SELECT game_id, home_team_id, home_team_name, away_team_id, away_team_name, start_time, home_pts, away_pts FROM GAMES"

query4 = "SELECT position, name, wins, losses, win_pct, ppg FROM TEAMS WHERE conference = 'East' ORDER BY position"

query5 = "SELECT position, name, wins, losses, win_pct, ppg FROM TEAMS WHERE conference = 'West' ORDER BY position"

query6 = "SELECT id, rank, name, team_id, team_name, pts FROM PLAYERS ORDER BY rank"

query7 = "SELECT id, rank, name, team_id, team_name, pts, min, fgm, fg_pct FROM PLAYERS ORDER BY rank"

query8 = "SELECT id, rank, name, team_id, team_name, pts FROM PLAYERS ORDER BY rank"

query9 = "SELECT id, rank, name, team_id, team_name, pts, min, fgm, fg_pct FROM PLAYERS ORDER BY rank"

query10 = "SELECT * FROM GAMESLOG WHERE GAME_ID = ?"

query11 = "SELECT game_id, home_team_id, home_team_tricode, home_team_name, away_team_id, away_team_tricode, away_team_name, start_time, game_label, arena_name, arena_city FROM GAMES WHERE start_time >= '2024-03-01'"

query12 = "SELECT starting_strength FROM TEAMS WHERE id = ?"

query13 = "SELECT strength FROM TEAMS WHERE id = ?"