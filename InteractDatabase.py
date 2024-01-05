# pip install mysql-connector-python
import pandas as pd
import mysql.connector


def convertir_a_datetime(dataframe):
    for columna in dataframe.columns:

        if columna == "fecha":
            try:
                # Intentar convertir la columna a datetime
                dataframe[columna] = pd.to_datetime(dataframe[columna])
            except (TypeError, ValueError):
                # Ignorar columnas que no se pueden convertir a datetime
                pass

    # Devolver el DataFrame después de intentar convertir todas las columnas
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


# Función que devuelve lista de columnas de la tabla y lista de placeholders para la query
def get_columns_and_placeholders(cursor, table_name):

    cursor.execute(f"DESCRIBE {table_name}")
    column_descriptions = cursor.fetchall()

    # Extraer los nombres de las columnas y determinar si alguna columna es autoincremental
    table_columns = []
    for column_info in column_descriptions:
        column_name = column_info[0]
        is_auto_increment = column_info[5] == 'auto_increment'
        if not is_auto_increment:
            table_columns.append(column_name)

    # Construir la lista de columnas para la consulta (excluyendo la que sea autoincremental)
    columns_list = ', '.join(table_columns)
    # Construir la lista de marcadores de posición %s para la consulta
    placeholders = ', '.join(['%s' for _ in table_columns])

    return [table_columns, columns_list, placeholders]


def verify_existing_value_in_file(cursor, table_name, table_columns, row):

    # Verificar si ya existe una fila con los mismos valores
    existing_query = f"SELECT * FROM {table_name} WHERE "
    conditions = [f"{column} = %s" for column in table_columns]
    existing_query += ' AND '.join(conditions)

    existing_values = [row[column] for column in table_columns]

    cursor.execute(existing_query, tuple(existing_values))

    return [cursor.fetchone(), existing_values]


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
    def create_foreign_keys(self, table_name, foreign_keys):
        print(f"Creating foreign keys in {table_name}...")

        queries = []
        for column_name, foreign_table, foreign_column in foreign_keys:
            query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} INT, ADD CONSTRAINT fk_{table_name}_{column_name} " \
                    f"FOREIGN KEY ({column_name}) REFERENCES {foreign_table}({foreign_column}) ON DELETE CASCADE;"
            queries.append(query)

        try:
            with self.connection.cursor() as cursor:
                for query in queries:
                    cursor.execute(query)

            print("Foreign keys created")
            return True

        except mysql.connector.Error as error:
            print("Error creating foreign keys:", error)
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
    def insert_data_table_from_dataframe(self, table_name, data_frame):
        data = convertir_a_datetime(data_frame)

        cursor = self.connection.cursor()

        try:
            [table_columns, columns_list, placeholders] = get_columns_and_placeholders(cursor, table_name)

            # Iterar sobre los datos y ejecutar la inserción en la base de datos
            for index, row in data.iterrows():

                [value_exist, existing_values] = verify_existing_value_in_file(cursor, table_name, table_columns, row)

                # Si ya existe una fila, omitir la inserción
                if value_exist:
                    continue

                # Utilizar parámetros de sustitución (%s) en la consulta para evitar SQL injection
                query = f"INSERT INTO {table_name} ({columns_list}) VALUES ({placeholders})"

                # Ejecutar la consulta con los valores correspondientes
                cursor.execute(query, tuple(existing_values))

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

    def getIdFromTable(self, table_name, column_name, data):

        cursor = self.connection.cursor()
        users = []
        try:

            for index, row in data.iterrows():
                query = f"SELECT id FROM {table_name} WHERE {column_name} = '{row[column_name]}'"
                cursor.execute(query)
                result = cursor.fetchone()

                if result:
                    user_id = result[0]
                    users.append({'id':user_id, column_name: row[column_name]})
                else:
                    print(f"No se encontró un usuario con el nombre '{row[column_name]}'.")

            return users
        except Exception as e:
            print(f"Error al insertar datos en la tabla '{table_name}': {e}")

    def insert_data_table_from_json(self, table_name, data_frame):

        cursor = self.connection.cursor()

        try:

            [table_columns, columns_list, placeholders] = get_columns_and_placeholders(cursor, table_name)

            # Iterar sobre los datos y ejecutar la inserción en la base de datos
            for data in data_frame:

                user_id = data['user_id']
                tweet_id = data['tweet_id']

                #[value_exist, existing_values] = verify_existing_value_in_file(cursor, table_name, table_columns, user_id)
#
                #print(value_exist)
                #print(existing_values)
#
                ## Si ya existe una fila, omitir la inserción
                #if value_exist:
                #    continue

                # Utilizar parámetros de sustitución (%s) en la consulta para evitar SQL injection
                query = f"INSERT INTO {table_name} ({columns_list}) VALUES ({placeholders})"

                # Ejecutar la consulta con los valores correspondientes
                cursor.execute(query, tuple((user_id, tweet_id)))
                self.connection.commit()
                # Cerrar cursor y conexión
            cursor.close()
            self.connection.close()
            print("Data inserted successfully")

        except Exception as e:
            # En caso de error, realizar un rollback para deshacer los cambios
            self.connection.rollback()
            print(f"Error inserting user-tweet data: {e}")
