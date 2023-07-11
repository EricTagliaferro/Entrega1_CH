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

    def loads(self):
        import pandas as pd

        Top20 = self.spark.read.format("csv").option("header",True).option("inferSchema",True).option("delimiter", ",").load("/opt/airflow/Data/Data_filtered.csv")
        Top20.show()
        
        Top20.write \
            .format("jdbc") \
            .option("url", env['REDSHIFT_URL']) \
            .option("dbtable", f"{env['REDSHIFT_SCHEMA']}.nbastat3") \
            .option("user", env['REDSHIFT_USER']) \
            .option("password", env['REDSHIFT_PASSWORD']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(">>> [L] Datos cargados exitosamente")

if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_Users()
    etl.run()
