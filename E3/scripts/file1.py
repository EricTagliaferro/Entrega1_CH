import os
from pathlib import Path
import requests
from datetime import datetime, timedelta
from os import environ as env

from pyspark.sql.functions import concat, col, lit, when, expr, to_date

from commons import ETL_Spark

#dag_path = os.getcwd()

class ETL_Users(ETL_Spark):
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")

    def run(self):
        process_date = "2023-07-09"  # datetime.now().strftime("%Y-%m-%d")
        self.execute(process_date)

    def extract(self):
        """
        Extrae datos de la API
        """
        print(">>> [E] Extrayendo datos de la API...")
        test_url = "https://stats.nba.com/stats/leagueLeaders?LeagueID=00&PerMode=PerGame&Scope=S&Season=2022-23&SeasonType=Playoffs&StatCategory=PTS"
        r =requests.get(url=test_url).json()
        Columnas = r['resultSet']["headers"]
        Tabla = self.spark.createDataFrame(data = r['resultSet']["rowSet"],schema=Columnas) 
        Tabla.printSchema()
        Tabla.show()
        Tabla.write.mode('overwrite').option("header",True).csv("/opt/airflow/Data/Data_raw.csv")

if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_Users()
    etl.run()
