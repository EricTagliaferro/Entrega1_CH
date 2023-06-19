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

#Reentregable E2

En base a los comentarios del Profesor se modifico el codigo para solo utilizar Spark y no hacer el mix con Pandas. Asi que se mantiene la explicacion del entregable2 pero lo realizado con Pandas se hizo con Spark.




