import pandas as pd
import requests
import os
import redshift_connector
import numpy as np
from dotenv import load_dotenv

#Estadisticas de NBA
test_url = "https://stats.nba.com/stats/leagueLeaders?LeagueID=00&PerMode=PerGame&Scope=S&Season=2022-23&SeasonType=Playoffs&StatCategory=PTS"
r =requests.get(url=test_url).json()


#Dar formato, seleccionando algunas columnas y las primeras 20 filas 
Columnas = r['resultSet']["headers"]
Tabla = pd.DataFrame(r['resultSet']["rowSet"],columns=Columnas)
Tabla2 = Tabla.applymap(lambda x: x.replace("'", "") if (isinstance(x, str)) else x)
Tablaresumen = Tabla2[['PLAYER_ID', 'RANK','PLAYER', 'TEAM_ID', 'TEAM', 'GP', 'REB', 'AST', 'STL', 'BLK']]
Top20 = Tablaresumen[Tablaresumen["RANK"]<20]

#Se cargaron las credenciales en un archivo .env

load_dotenv()
hostrs = os.getenv("HOSTRS")
user = os.getenv("USER")
passw = os.getenv("PASSW")
db = os.getenv("DB")
sch = os.getenv("SCH")


#Conectar a Redshift

connection = redshift_connector.connect( 
    host=hostrs, 
    database=db, 
    port=int("5439"), 
    user=user, 
    password=passw,
)
my_schema = sch

cursor = connection.cursor()

connection.rollback()
connection.autocommit = True

#Crear Tabla
cursor.execute(f"CREATE TABLE {my_schema}.NBAstat (PLAYER_ID float, RANK float, PLAYER varchar, TEAM_ID float , TEAM varchar, GP float, REB float, AST float, STL float, BLK float)")

#insertar datos

for index, row in Top20.iterrows():
    cursor.execute(f"INSERT INTO {my_schema}.NBAstat VALUES ({row['PLAYER_ID']},{row['RANK']},'{str(row['PLAYER'])}',{row['TEAM_ID']},'{row['TEAM']}',{row['GP']},{row['REB']},{row['AST']},{row['STL']},{row['BLK']})")
