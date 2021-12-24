import urllib.request, json, sqlite3
from datetime import datetime, date
from flask import Flask, request, render_template
app = Flask(__name__)

TEAMS_URL = 'https://global.nba.com/feeds/awards/team/'
STATS_URL = 'https://global.nba.com/stats2/league/playerstats.json'
SQL_DB_FILE = 'core.db'

def calculateAge(birthDate):
    today = date.today()
    age = today.year - birthDate.year - ((today.month, today.day) < (birthDate.month, birthDate.day))
    return age

def createTable(cursor):
    CREATE_TEAMS_TABLE = ''' 
        CREATE TABLE "teams" (
            "team_id"	INTEGER,
            "season"	TEXT,
            "name"	TEXT,
            "city"	TEXT,
            "conference"	TEXT,
            "coach"	BLOB,
            "arena"	TEXT,
            "yearFounded"	INTEGER,
            "abbr"	INTEGER,
            "division"	TEXT,
            "nba_team_id"	INTEGER,
            PRIMARY KEY("team_id" AUTOINCREMENT)
        );
    '''

    CREATE_PLAYERS_TABLE = '''
        CREATE TABLE "players" (
            "player_id"	INTEGER,
            "team_id"	INTEGER,
            "rank"	INTEGER,
            "first_name"	TEXT,
            "last_name"	TEXT,
            "height"	TEXT,
            "jersey_number"	INTEGER,
            "weight"	TEXT,
            "age"	INTEGER,
            "country"	TEXT,
            "experience"	INTEGER,
            "position"	INTEGER,
            "schoolType"	TEXT,
            "draftYear"	INTEGER,
            "nba_player_id"	INTEGER,
            PRIMARY KEY("player_id" AUTOINCREMENT),
            FOREIGN KEY("team_id") REFERENCES "teams"("team_id")
        );
    '''

    try:
        cursor.execute(CREATE_TEAMS_TABLE)
        cursor.execute(CREATE_PLAYERS_TABLE)
    except:
        None

    print("Table Created -- SUCCESS")

def fetchTeam(cursor, team):
    cursor = cursor.execute("SELECT team_id, nba_team_id from teams WHERE nba_team_id = " + team['id'])
    result = cursor.fetchone()
    if result:
        # print("Team Exists")
        return result[0]
    else:
        # print("Team Doesn't Exists")
        global season
        with urllib.request.urlopen(TEAMS_URL + team['id'] + '.json') as url:
            data = json.loads(url.read().decode())
        sqlQuery = ''' INSERT INTO teams (season,name,city,conference,abbr,division,yearFounded,arena,coach,nba_team_id) VALUES (?,?,?,?,?,?,?,?,?,?) '''
        sqlValues = (season,team['name'], team['city'], team['conference'], team['abbr'], team['division'], data['resultSets'][0]['rowSet'][0][3], data['resultSets'][0]['rowSet'][0][5], data['resultSets'][0]['rowSet'][0][9],team['id'])
        cursor.execute(sqlQuery,sqlValues)
        return cursor.lastrowid
    # print(team['id'])
    # print(team['name'])
    # print(team['city'])
    # print(team['conference'])
    # print(team['abbr'])
    # print(team['division'])
    # print(data['resultSets'][0]['rowSet'][0][3])    # Year Founded
    # print(data['resultSets'][0]['rowSet'][0][5])    # Arena
    # print(data['resultSets'][0]['rowSet'][0][9])    # Coach

def fetchPlayers(cursor, players):
    for player in players:
        cursor = cursor.execute("SELECT nba_player_id from players WHERE nba_player_id = " + player['playerProfile']['playerId'])
        result = cursor.fetchone()
        if result:
            pass
        else:
            # print(player['playerProfile']['playerId'])
            # print(player['playerProfile']['firstName'])
            # print(player['playerProfile']['lastName'])
            # print(player['playerProfile']['height'])
            # print(player['playerProfile']['jerseyNo'])
            # print(player['playerProfile']['weight'])
            # print(player['playerProfile']['country'])
            # print(player['playerProfile']['experience'])
            # print(player['playerProfile']['position'])
            # print(player['playerProfile']['schoolType'])
            # print(player['playerProfile']['draftYear'])
            DOB = datetime.strptime(datetime.fromtimestamp(int(player['playerProfile']['dob'])/1000).strftime('%Y-%m-%d'), '%Y-%m-%d')
            AGE = calculateAge(DOB)    #Age
            team_id = fetchTeam(cursor, player['teamProfile'])
            sqlQuery = ''' INSERT INTO players (nba_player_id,team_id,first_name,last_name,height,jersey_number,weight,age,country,experience,position,schoolType,draftYear,rank) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
            sqlValues = (player['playerProfile']['playerId'],team_id, player['playerProfile']['firstName'], player['playerProfile']['lastName'], player['playerProfile']['height'], player['playerProfile']['jerseyNo'], player['playerProfile']['weight'], AGE, player['playerProfile']['country'], player['playerProfile']['experience'], player['playerProfile']['position'], player['playerProfile']['schoolType'], player['playerProfile']['draftYear'], player['rank'])
            cursor.execute(sqlQuery,sqlValues)

with urllib.request.urlopen(STATS_URL) as url:
    data = json.loads(url.read().decode())  ## Parsing JSON Captured Data
    season = data['payload']['season']['yearDisplay']   ## Storing Season into a Var, As its constant program-wide

    conn = sqlite3.connect(SQL_DB_FILE)   ## Connecting to SQLite3 DB
    cursor = conn.cursor()  ## Creating a Cursor

    createTable(cursor) ## Create Table

    print("Opened database -- SUCCESS")
    print("Data Scrapping -- In Progress")
    fetchPlayers(cursor, data['payload']['players'])    ## Calling fetchPlayers() to begin storing data
    conn.commit()   ## Commiting changes to SQLite3 DB
    print("Data Scrapping -- SUCCESS")
    conn.close()    ## Closing the Connection
    # print("Program Executed -- SUCCESS")
    print("Starting Web Server...")

@app.route('/')
def display():
    conn = sqlite3.connect(SQL_DB_FILE)   ## Connecting to SQLite3 DB
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()  ## Creating a Cursor
    cursor.execute("SELECT * FROM teams")
    teams = cursor.fetchall()
    cursor.execute("SELECT * FROM players")
    players = cursor.fetchall()
    return render_template("display.html",teams = teams, players = players)

if __name__ == '__main__':
    app.run()
    conn.close()