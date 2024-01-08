# pip install mysql-connector-python
import pandas as pd
import mysql.connector
from datetime import date

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
def determine_column_type_and_max_dataframe(column_name, column):

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


def determine_column_type_and_max_json(column_name, column):

    try:
        if column_name == "fecha":
            return "DATE", ""
        elif isinstance(column, int):
            return "INT", "(10)"
        elif isinstance(column, float):
            return "FLOAT", ""
        elif isinstance(column, str):
            return "VARCHAR", "(255)"
        elif isinstance(column, date):
            return "DATE", ""
        elif isinstance(column, bool):
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


# Función que verifica si ya existe una fila con los mismos valores
def verify_existing_value_in_file(cursor, table_name, table_columns, row):

    existing_query = f"SELECT * FROM {table_name} WHERE "
    conditions = [f"{column} = %s" for column in table_columns]
    existing_query += ' AND '.join(conditions)

    existing_values = [row[column] for column in table_columns]

    cursor.execute(existing_query, tuple(existing_values))

    return [cursor.fetchone(), existing_values]


class InteractMySQL:

    def __init__(self, connection):

        self.connection = connection

    def exportJsonFromTable(self, table_name):

        export = []
        try:

            with self.connection.cursor() as cursor:

                countquery = f"SELECT COUNT(*) FROM {table_name}"
                cursor.execute(countquery)
                count = cursor.fetchone()[0]

                for i in range(1, count + 1):
                    query = f"SELECT * FROM {table_name} WHERE id = {i}"
                    cursor.execute(query)
                    row = cursor.fetchone()

                    if row:
                        export_row = {}
                        column_names = [column[0] for column in cursor.description]

                        # Iterar sobre las columnas y sus valores
                        for column_name, column_value in zip(column_names, row):
                            export_row[column_name] = column_value

                        export.append(export_row)
                    else:
                        print(f"No se encontró un usuario con el nombre.")

            return export
        except Exception as e:
            print(f"Error al insertar datos en la tabla '{table_name}': {e}")

    # Crea la tabla con su clave primaria
    def create_table_with_primary_key(self, table_name, primary_key_name):

        print(f"-Creating table {table_name}...")
        query = f"CREATE TABLE {table_name} ({primary_key_name} INT(10) NOT NULL AUTO_INCREMENT PRIMARY KEY)"
        try:

            # El cursor se cerrará automáticamente cuando salga del bloque with, independientemente de si se produjo
            # una excepción o no.
            with self.connection.cursor() as cursor:
                cursor.execute(query)

            print(f"-Table {table_name} created.")
            return True

        except mysql.connector.Error as error:

            print(f"Error creating table {table_name}:", error)
            return False

    # Crea una columna en la tabla (dataframe)
    def create_column_from_data(self, table_name, column_name, data):

        if isinstance(data, list):
            # Obtener la columna del Json
            column_data = data[0][column_name]
            column_type, column_max = determine_column_type_and_max_json(column_name, column_data)
        else:
            # Obtener la columna del DataFrame
            column_data = data[column_name]
            # Determinar automáticamente el tipo y máximo de la columna
            column_type, column_max = determine_column_type_and_max_dataframe(column_name, column_data)

        print(f"Creating column {column_name} in {table_name} type {column_type} max {column_max}...")
        if column_max:
            query = f"ALTER TABLE {table_name} ADD {column_name} {column_type}{column_max} NOT NULL"
        else:
            query = f"ALTER TABLE {table_name} ADD {column_name} {column_type} NOT NULL"

        try:

            with self.connection.cursor() as cursor:
                cursor.execute(query)

            print(f"Column {column_name} created in {table_name}")
            return True

        except mysql.connector.Error as error:
            print("Error creating column:", error)
            return False

    # Crea claves foraneas
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

    # Completa la tabla con datos del array de objetos
    def insert_data_table_from_json(self, table_name, data):

        print(f"...Insertando datos en tabla: {table_name}, a partir de un json.")
        cursor = self.connection.cursor()

        try:
            # print("a) Obteniendo lista de columnas y placeholders para realizar la query.")
            [table_columns, columns_list, placeholders] = get_columns_and_placeholders(cursor, table_name)

            # print("b) Iterando sobre los elementos del array e insertando cada uno.")
            # Iterar sobre los datos y ejecutar la inserción en la base de datos
            for item in data:

                #print(f"b.1) Verificando existencia de la fila {item} dentro de la tabla {table_name}")
                [value_exist, existing_values] = verify_existing_value_in_file(cursor, table_name, table_columns, item)

                #print(f"Elemento: {item} {'repetido' if value_exist else 'nuevo'}")

                # Si ya existe una fila, omitir la inserción
                if value_exist:
                    # print(f"Omitiendo: {item}...")
                    continue

                # Utilizar parámetros de sustitución (%s) en la consulta para evitar SQL injection
                query = f"INSERT INTO {table_name} ({columns_list}) VALUES ({placeholders})"

                list_values = []
                for clave, valor in item.items():
                    list_values.append(valor)

                # Ejecutar la consulta con los valores correspondientes
                cursor.execute(query, tuple(list_values))
                self.connection.commit()

            # Cerrar cursor y conexión
            cursor.close()
            self.connection.close()
            print("Data inserted successfully")

        except Exception as e:
            # En caso de error, realizar un rollback para deshacer los cambios
            self.connection.rollback()
            print(f"Error inserting user-tweet data: {e}")

    # -------

    # Crea una tabla, crea las columnas de la lista e inserta los datos de un dataFrame
    def create_insert_data_table_columns_from_data(self, table_name, primary_key_name, list_columns, data_frame):

        try:
            self.create_table_with_primary_key(table_name, primary_key_name)

            for column_name in list_columns:

                if column_name in data_frame:
                    self.create_column_from_data(table_name, column_name, data_frame)
                else:
                    if all(column_name in d for d in data_frame):
                        self.create_column_from_data(table_name, column_name, data_frame)

            if isinstance(data_frame, list):
                self.insert_data_table_from_json(table_name, data_frame)
            else:
                self.insert_data_table_from_dataframe(table_name, data_frame)

        except Exception as e:

            print(f"Error: {e}")
