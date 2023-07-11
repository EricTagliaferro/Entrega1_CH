# Este script está pensado para correr en Spark y hacer el proceso de ETL de la tabla users
import os
import requests
from datetime import datetime, timedelta
from os import environ as env

from pyspark.sql.functions import concat, col, lit, when, expr, to_date
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import regexp_replace,col

from commons import ETL_Spark
dag_path = os.getcwd()  

class ETL_Users(ETL_Spark):
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")

    def run(self):
        process_date = "2023-07-09"  # datetime.now().strftime("%Y-%m-%d")
        self.execute(process_date)

    def extract(self):

        print(">>> [T] Transformando datos...")
        import pandas as pd
        #TablaP = pd.read_csv(dag_path+"/Data/Data_raw.csv")
        #print(TablaP)
        
        #Tabla = self.spark.createDataFrame(data = TablaP)
        #Tabla.show()
        Tabla = self.spark.read.format("csv").option("header",True).option("inferSchema",True).option("delimiter", ",").load("/opt/airflow/Data/Data_raw.csv")
        TablacheckC = Tabla
        TablacheckC.dropDuplicates(['PLAYER'])
        TablacheckC.count()
        w = Window.partitionBy('TEAM')
        Tabla = Tabla.withColumn('TEAM_PLAYERS', F.count('TEAM').over(w))
        Tabla = Tabla.withColumn('SUM_GP_TEAM', F.sum('GP').over(w))

        Tabla = Tabla.withColumn('PLAYER', regexp_replace('PLAYER',"'",""))

        Tablaresumen = Tabla.select('PLAYER_ID', 'RANK','PLAYER', 'TEAM_ID', 'TEAM', 'GP', 'REB', 'AST', 'STL', 'BLK','TEAM_PLAYERS','SUM_GP_TEAM')
        Tablaresumen = Tablaresumen.sort("RANK")
        Top20 = Tablaresumen[Tablaresumen["RANK"]<20]
        Top20.show()
        Top20.write.mode('overwrite').option("header",True).csv("/opt/airflow/Data/Data_filtered.csv")


if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_Users()
    etl.run()
