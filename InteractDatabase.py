# pip install mysql-connector-python
import pandas as pd
import mysql.connector


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
    try:
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

    except:
        return "VARCHAR", "(255)"


class InteractMySQL:

    def __init__(self, connection):

        self.connection = connection

    # Crea la tabla con su clave primaria
    def create_table_with_primary_key(self, table_name, primary_key_name):

        print("Creating table..." + table_name)
        query = f"CREATE TABLE {table_name} ({primary_key_name} INT(10) NOT NULL AUTO_INCREMENT PRIMARY KEY)"
        try:

            # El cursor se cerrará automáticamente cuando salga del bloque with, independientemente de si se produjo una excepción o no.
            with self.connection.cursor() as cursor:
                cursor.execute(query)

            print("Table created")
            return True

        except mysql.connector.Error as error:

            print("Error creating table:", error)
            return False

    # Crea una clave foranea
    def create_foreign_key(self, table_name, column_name, foreign_table, foreign_column):
        print(f"Creating foreign key in {table_name}/{column_name} referencing {foreign_table}/{foreign_column}...")

        query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} INT, ADD CONSTRAINT fk_{table_name} " \
                f"FOREIGN KEY ({column_name}) REFERENCES {foreign_table}({foreign_column}) ON DELETE CASCADE;"

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)

            print("Foreign key created")
            return True

        except mysql.connector.Error as error:
            print("Error creating foreign key:", error)
            return False

    # Crea una columna en la tabla
    def create_column(self, table_name, column_name, data_frame):

        # Obtener la columna del DataFrame
        column_data = data_frame[column_name]

        # Determinar automáticamente el tipo y máximo de la columna
        column_type, column_max = determine_column_type_and_max(column_name, column_data)

        print(f"Creating column in {table_name}/{column_name} type {column_type} max {column_max}...")
        if column_max:
            query = f"ALTER TABLE {table_name} ADD {column_name} {column_type}{column_max} NOT NULL"
        else:
            query = f"ALTER TABLE {table_name} ADD {column_name} {column_type} NOT NULL"

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)

            print("Column created")
            return True

        except mysql.connector.Error as error:
            print("Error creating column:", error)
            return False

    # Completa la tabla con datos del data_frame
    def insert_data_table_from_json(self, table_name, data_frame):

        data = convertir_a_datetime(data_frame)

        cursor = self.connection.cursor()

        try:

            # Obtener las columnas de la tabla
            cursor.execute(f"DESCRIBE {table_name}")
            column_descriptions = cursor.fetchall()

            # Extraer los nombres de las columnas
            table_columns = []
            for column_info in column_descriptions:
                column_name = column_info[0]
                table_columns.append(column_name)

            # Construir la lista de columnas para la consulta
            columns_list = ', '.join(table_columns)
            # Construir la lista de marcadores de posición %s para la consulta
            placeholders = ', '.join(['%s' for _ in table_columns])

            # Iterar sobre los datos y ejecutar la inserción en la base de datos
            for index, row in data.iterrows():
                # Utilizar parámetros de sustitución (%s) en la consulta para evitar SQL injection
                query = f"INSERT INTO {table_name} ({columns_list}) VALUES ({placeholders})"

                # Obtener los valores de las columnas para el índice actual
                values = [row[column] for column in table_columns]

                # Ejecutar la consulta con los valores correspondientes
                cursor.execute(query, tuple(values))

            # Confirmar los cambios en la base de datos fuera del bucle
            self.connection.commit()
            print("Data inserted successfully")

        except Exception as e:
            # En caso de error, realizar un rollback para deshacer los cambios
            self.connection.rollback()
            print(f"Error inserting data: {e}")

        finally:
            # Cerrar el cursor al finalizar
            cursor.close()

    def insert_data_table_from_json_with_foreign_key(self, table_name, foreign_key_column, key_column, data_frame):

        data = convertir_a_datetime(data_frame)

        if key_column in data.columns:
            data[foreign_key_column] = data[key_column]

        cursor = self.connection.cursor()

        try:

            # Obtener las columnas de la tabla
            cursor.execute(f"DESCRIBE {table_name}")
            column_descriptions = cursor.fetchall()

            # Extraer los nombres de las columnas
            table_columns = []
            for column_info in column_descriptions:
                column_name = column_info[0]
                table_columns.append(column_name)

            # Construir la lista de columnas para la consulta
            columns_list = ', '.join(table_columns)
            # Construir la lista de marcadores de posición %s para la consulta
            placeholders = ', '.join(['%s' for _ in table_columns])

            # Iterar sobre los datos y ejecutar la inserción en la base de datos
            for index, row in data.iterrows():
                # Utilizar parámetros de sustitución (%s) en la consulta para evitar SQL injection
                query = f"INSERT INTO {table_name} ({columns_list}) VALUES ({placeholders})"

                # Obtener los valores de las columnas para el índice actual
                values = [row[column] for column in table_columns]
                print(values) #[Timestamp('1970-01-01 00:00:00.000000001'), 'La tecnología de la realidad virtual está revolucionando la forma en que experimentamos los videojuegos. #tecnología #videojuegos', '2023-03-13 16:45:00', 23, 87, Timestamp('1970-01-01 00:00:00.000000001')]

                # Ejecutar la consulta con los valores correspondientes
                cursor.execute(query, tuple(values))

            # Confirmar los cambios en la base de datos fuera del bucle
            self.connection.commit()
            print("Data inserted successfully")

        except Exception as e:
            # En caso de error, realizar un rollback para deshacer los cambios
            self.connection.rollback()
            print(f"Error inserting data: {e}")

        finally:
            # Cerrar el cursor al finalizar
            cursor.close()