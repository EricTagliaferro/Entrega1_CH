# Entregable 1

1)	Se extrajo la información de estadísticas de la NBA en formato jason de un API utilizando la librería request.
2)	Se le dio formato a la Tabla con Pandas, seleccionando las columnas necesarias, recortando la cantidad de registro.
3)	Se armo un archivo .env con las credenciales.
4)	Se realizo la conexión con Redshift, con la librería “redshift_connector” y se trajo las credenciales del archivo .env con “load_dotenv”.
5)	Se creo el cursor y con “cursor.execute” se utilizo una query de lenguaje SQL para crear la Tabla en Redshift, identificando las columnas y sus tipos:

cursor.execute(f"CREATE TABLE {my_schema}.NBAstat (PLAYER_ID float, RANK float, PLAYER varchar, TEAM_ID float , TEAM varchar, GP float, REB float, AST float, STL float, BLK float)")

6)	Luego se insertó la información en la Tabla por iteración y con INSER INTO

# Entregable 2

En función a los comentarios recibidos en el primer entregable modifique la conexión a Redshift utilizada. Se realizo un chequeo de duplicados y se agregue 2 variables (suma y counts).
1)	Se extrajo la información de estadísticas de la NBA en formato jason de un API utilizando la librería request.
2)	Se le dio formato a la Tabla con Pandas, seleccionando las columnas necesarias, recortando la cantidad de registro.
Se realizo un chequeo de duplicados y se agregaron 2 variables (suma y counts).
3)	Se armo un archivo .env con las credenciales.
4)	Se creo una sesión de Sparks y un dataframe en base a la tabla armada con pandas
5)	Se creo la conexión con psycopg2
6)	Se creo la Tabla indicando los tipos y agregándole el comando de “if not exist” para que verifique primero si la tabla ya esta generada.
7)	Con Spark.Write se cargo la información de la Tabla con las estadísticas de NBA en la Tabla de Redshift.

Reentrega E2

En base a los comentarios del Profesor se modifico el codigo para solo utilizar Spark y no hacer el mix con Pandas. Asi que se mantiene la explicacion del entregable2 pero lo realizado con Pandas se hizo con Spark.

# Entregable 3

En la presente entrega se procedera a generar un Docker file que permita correr Airflow con conexion a Spark y Redshift, con el fin de lograr extraer informacion de NBA de un API, transformar la data y luego cargarla en mi database de Redshift

Los archivos a tener en cuenta son:

- docker_images/: Contiene los Dockerfiles para crear las imagenes utilizadas de Airflow y Spark.
- docker-compose.yml: Archivo de configuración de Docker Compose. Contiene la configuración de los servicios de Airflow y Spark.
- .env: Archivo de variables de entorno. Contiene variables de conexión a Redshift y driver de Postgres.
- dags/: Carpeta con los archivos de los DAGs.
- Entregable.py: DAG principal que ejecuta el pipeline de extracción, transformación y carga de datos de usuarios.
- Data : Carpeta donde se va descargado las bases de cada paso del ETL
- logs/: Carpeta con los archivos de logs de Airflow.
- plugins/: Carpeta con los plugins de Airflow.
- postgres_data/: Carpeta con los datos de Postgres.
- scripts/: Carpeta con los scripts de Spark.
- postgresql-42.5.2.jar: Driver de Postgres para Spark.
- common.py: Script de Spark con funciones comunes.
- file1.py: Script de Spark que extrae la info de la API.
- file2.py: Script de Spark que transforma la informacion
- file3.py: Script de Spark que carga la informacion en la DB de Redshift.

# Pasos para ejecutar

  1. Descagarse las carpeta E3 que contiene los archivo mencionados anteriormente
  2. Crear las siguientes carpetas a la misma altura del docker-compose.yml
     
     `mkdir -p dags,logs,plugins,postgres_data,scripts`
  3. Posicionarse en la carpeta E3. A esta altura debería ver el archivo docker-compose.yml.
  4. Crear un archivo con variables de entorno llamado .env ubicado a la misma altura que el docker-compose.yml. Cuyo contenido sea:
```
  REDSHIFT_HOST=...
  REDSHIFT_PORT=5439
  REDSHIFT_DB=...
  REDSHIFT_USER=...
  REDSHIFT_SCHEMA=...
  REDSHIFT_PASSWORD=...
  REDSHIFT_URL="jdbc:postgresql://${REDSHIFT_HOST}:${REDSHIFT_PORT}/${REDSHIFT_DB}?user=${REDSHIFT_USER}&password=${REDSHIFT_PASSWORD}"
  DRIVER_PATH=/tmp/drivers/postgresql-42.5.2.jar
 ```  
  5. Las imagenes fueron generadas a partir de los Dockerfiles ubicados en docker_images/. Generar las imagenes, ejecutar los comandos que están en los Dockerfiles.
     
 ```
  docker-compose up --build
 ```  
6. Una vez que los servicios estén levantados, ingresar a Airflow en http://localhost:8080/.
7. En la pestaña Admin -> Connections crear una nueva conexión con los siguientes datos para Redshift:
   + Conn Id: `redshift_default`
   + Conn Type: `Amazon Redshift`
   + Host: `host de redshift`
   + Database: `base de datos de redshift`
   + Schema: `esquema de redshift`
   + User: `usuario de redshift`
   + Password: `contraseña de redshift`
   + Port: `5439`
8. En la pestaña Admin -> Connections crear una nueva conexión con los siguientes datos para Spark:
   + Conn Id: `spark_default`
   + Conn Type: `Spark`
   + Host: `spark://spark`
   + Port: `7077`
   + Extra: `{"queue": "default"}`
9. En la pestaña Admin -> Variables crear una nueva variable con los siguientes datos:
   + Key: `driver_class_path`
   + Value: `/tmp/drivers/postgresql-42.5.2.jar`
10. En la pestaña Admin -> Variables crear una nueva variable con los siguientes datos:
    + Key: `spark_scripts_dir`
    + Value: `/opt/airflow/scripts`
11. Ejecutar el DAG `Entregable3_Eric`
12. Ejecutar las tareas en el orden dado:
    + Extraer_info
    + create_table
    + Transformar_data
    + Enviar_Redshift
      
# Entregable 4

En la presente entrega se procedera a generar un Docker file que permita correr Airflow con conexion a Spark y Redshift, con el fin de lograr extraer informacion de NBA de un API, transformar la data y luego cargarla en mi database de Redshift

Los archivos a tener en cuenta son:

- docker_images/: Contiene los Dockerfiles para crear las imagenes utilizadas de Airflow y Spark.
- docker-compose.yml: Archivo de configuración de Docker Compose. Contiene la configuración de los servicios de Airflow y Spark.
- .env: Archivo de variables de entorno. Contiene variables de conexión a Redshift y driver de Postgres.
- dags/: Carpeta con los archivos de los DAGs.
- Entregable_Eric.py: DAG principal que ejecuta el pipeline de extracción, transformación y carga de datos de usuarios.
- Data : Carpeta donde se va descargado las bases de cada paso del ETL
- logs/: Carpeta con los archivos de logs de Airflow.
- plugins/: Carpeta con los plugins de Airflow.
- postgres_data/: Carpeta con los datos de Postgres.
- scripts/: Carpeta con los scripts de Spark.
- postgresql-42.5.2.jar: Driver de Postgres para Spark.
- common.py: Script de Spark con funciones comunes.
- Extraer_info.py: Script de Python que extrae la info de la API.
- Trasnformar_info.py: Script de Python que transforma la informacion.
- Crea_tabla.py: Script de Python que crea la Tabla en Redshift con la infromacion cargada en el archivo .env.
- Cargar_info.py: Script de Python que carga la informacion de la Database generda dentro de la Tabla que esta en Redshift.

# Pasos para ejecutar

  1. Descagarse las carpeta Entregafinal_Eric que contiene los archivo mencionados anteriormente
  2. Posicionarse en la carpeta Entregafinal_Eric. A esta altura debería ver el archivo docker-compose.yml.
  3. Crear un archivo con variables de entorno llamado .env ubicado a la misma altura que el docker-compose.yml. Cuyo contenido sea:
```
  REDSHIFT_HOST=...
  REDSHIFT_PORT=5439
  REDSHIFT_DB=...
  REDSHIFT_USER=...
  REDSHIFT_SCHEMA=...
  REDSHIFT_PASSWORD=...
  REDSHIFT_URL="jdbc:postgresql://${REDSHIFT_HOST}:${REDSHIFT_PORT}/${REDSHIFT_DB}?user=${REDSHIFT_USER}&password=${REDSHIFT_PASSWORD}"
  DRIVER_PATH=/tmp/drivers/postgresql-42.5.2.jar
 ```  
  4. Las imagenes fueron generadas a partir de los Dockerfiles ubicados en docker_images/. Generar las imagenes, ejecutar los comandos que están en los Dockerfiles.
     
 ```
  docker-compose up --build
 ```  
5. Una vez que los servicios estén levantados, ingresar a Airflow en http://localhost:8080/.
6. En la pestaña Admin -> Connections crear una nueva conexión con los siguientes datos para Redshift:
   + Conn Id: `redshift_default`
   + Conn Type: `Amazon Redshift`
   + Host: `host de redshift`
   + Database: `base de datos de redshift`
   + Schema: `esquema de redshift`
   + User: `usuario de redshift`
   + Password: `contraseña de redshift`
   + Port: `5439`
7. En la pestaña Admin -> Connections crear una nueva conexión con los siguientes datos para Spark:
   + Conn Id: `spark_default`
   + Conn Type: `Spark`
   + Host: `spark://spark`
   + Port: `7077`
   + Extra: `{"queue": "default"}`
8. En la pestaña Admin -> Variables crear una nueva variable con los siguientes datos:
   + Key: `driver_class_path`
   + Value: `/tmp/drivers/postgresql-42.5.2.jar`
9. En la pestaña Admin -> Variables crear una nueva variable con los siguientes datos:
    + Key: `spark_scripts_dir`
    + Value: `/opt/airflow/scripts`
10. En la pestaña Admin -> Variables crear una nueva variable con los siguientes datos:
    + Key: `Threshold_tiempo`
    + Value: `1`
    + Puse el valor 1 como ejemplo se puede seleccionar otro threshold. Ver tarea "Transformar_data" en el punto 15)
11. En la pestaña Admin -> Variables crear una nueva variable con los siguientes datos:
    + Key: `SMTP_EMAIL_FROM`
    + Value: `Cargar el mail desde donde saldran las alertas`
12. En la pestaña Admin -> Variables crear una nueva variable con los siguientes datos:
    + Key: `SMTP_PASSWORD`
    + Value: `Cargar el password del mail cargado en la variable anterior`
13. En la pestaña Admin -> Variables crear una nueva variable con los siguientes datos:
    + Key: `SMTP_EMAIL_TO`
    + Value: `Cargar el mail donde llegaran las alerta`
14. Ejecutar el DAG `Entregable3_Eric`
15. Ejecutar las tareas en el orden dado:
    + Extraer_info
       - Descarga la info de NBA de una API, genera una Tabla con esta informacion y lo guarda en un CSV. En el momento que se descarga la info de la API se realiza un control del tiempo de descarga vs un threshold cargada en la variable del punto 10), si el tiempo de ejecucion en ms supera el threshold se envia un mail de aviso. 
    + Transformar_data
       - Se toma la informacion generada en la tarea anterior, se hace un chequeo de duplicados (enviando el resultado por email). Luego se hace una seleccion de columnas y se agregan 2 variables (suma y count). Finalemnte se selecionan los primeros 20 registros y se guarda en un csv.
    + Crear_tabla
       - Crear tabla en Redshift segun la informacion cargada en el archivo .env.
    + Enviar_Redshift
       - Cargar la db generada en la tarea "Transformar_info"
