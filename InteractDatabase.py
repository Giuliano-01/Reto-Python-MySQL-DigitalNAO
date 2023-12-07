# pip install mysql-connector-python
import time
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime


def convertir_a_datetime(dataframe):
    for columna in dataframe.columns:
        try:
            # Intentar convertir la columna a datetime
            dataframe[columna] = pd.to_datetime(dataframe[columna])
            return dataframe
        except (TypeError, ValueError):
            # Ignorar columnas que no se pueden convertir a datetime
            pass

    # Si no se encuentra ninguna columna válida, devolver el DataFrame original
    return dataframe


def choose_yesno():
    print("Yes (y) / No (n):")

    yes = {'yes', 'y', 'ye', ''}
    choice = input().lower()
    if choice in yes:
        return True
    else:
        return False


# Función para determinar el tipo y tamaño del campo basándonos en el valor del diccionario
def determine_column_type_and_max(column_name, column):

    if column_name == "fecha":
        return "DATE", ""
    elif pd.api.types.is_integer_dtype(column):
        return "INT", "(10)"
    elif pd.api.types.is_float_dtype(column):
        return "FLOAT", ""
    elif pd.api.types.is_string_dtype(column):
        return "VARCHAR", "(255)"
    elif pd.api.types.is_datetime64_any_dtype(column):
        return "DATE", ""
    elif pd.api.types.is_bool_dtype(column):
        return "BOOLEAN", ""
    else:
        # Puedes agregar más casos según sea necesario
        return "VARCHAR", "(255)"


class InteractMySQL:

    def __init__(self, connection):

        self.connection = connection

    # Crea la tabla con su clave primaria
    def create_table_with_primary_key(self, table_name, primary_key_name):

        print("Creating table..." + table_name)
        query = f"CREATE TABLE {table_name} ( {primary_key_name} INT(10) NOT NULL AUTO_INCREMENT PRIMARY KEY)"
        try:

            cursor = self.connection.cursor()
            cursor.execute(query)
            print("Table created")
            return True

        except mysql.connector.Error as error:
            print(error)
            return False

    # Crea una columna en la tabla
    def create_column(self, table_name, column_name, column_type, column_max):

        print("Creating column...")
        query = f"ALTER TABLE {table_name} ADD {column_name} {column_type}{column_max} NOT NULL"
        try:

            cursor = self.connection.cursor()
            cursor.execute(query)
            print("Alter created")
            return True

        except mysql.connector.Error as error:
            print(error)
            return False

    # Genera una tabla con sus columnas
    def create_table_from_data(self, table_name, data):

        new_data = convertir_a_datetime(data)

        # Asumimos que la primera columna es la clave primaria
        primary_key_name = new_data.columns[0]
        self.create_table_with_primary_key(table_name, primary_key_name)

        for column_name, column_data in new_data.items():

            #Determinamos el tipo y tamaño del campo basándonos en los tipos de datos reales
            column_type, column_max = determine_column_type_and_max(column_name, column_data)

            print("Column name: ", column_name, "Column type: ",column_type)
            # Creamos la columna en la tabla
            self.create_column(table_name, column_name, column_type, column_max)

    # Inserta los datos a una tabla desde un archivo
    def insert_data_from_json(self, table_name, data):

        print("Insert data in table from json...")

        try:

            # Iterar sobre cada fila del DataFrame
            for indice, fila in data.iterrows():
                texto = fila['texto']
                usuario = fila['usuario']
                hashtag = ', '.join(fila['hashtags']) if isinstance(fila['hashtags'], list) else fila['hashtags']
                fecha = fila['fecha']
                retweets = fila['retweets']
                favoritos = fila['favoritos']

                # Crear la consulta de inserción
                query_insert = f"INSERT INTO {table_name} (texto, usuario, hashtags, fecha, retweets, favoritos) VALUES (%s, %s, %s, %s, %s, %s)"
                valores = (texto, usuario, hashtag, fecha, retweets, favoritos)

                # Ejecutar la consulta de inserción
                cursor = self.connection.cursor()
                cursor.execute(query_insert, valores)

            # Confirmar los cambios en la base de datos
            self.connection.commit()

        except mysql.connector.Error as error:

            if error.errno == errorcode.ER_TABLE_EXISTS_ERROR:

                print("Table already exists. Skipping...")

            else:

                print("Error: ", error)
                return False
        finally:

            print("Success")
