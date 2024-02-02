import mysql.connector
from InteractionDatabase.Utils import *


class InteractMySQL:

    def __init__(self, connection):

        self.connection = connection

    def getcolumnvalue_from_other_columnvalue(self, table_name, column_name, value_name, return_column):

        query = f"SELECT {return_column} FROM {table_name} WHERE {column_name} = '{value_name}'"
        try:

            with self.connection.cursor() as cursor:
                cursor.execute(query)
                # Recuperar los resultados
                result = cursor.fetchall()
                # Verificar si hay resultados
                if result:
                    return result  # Devolver el valor de la columna deseada
                else:
                    return None

        except mysql.connector.Error as error:
            print(error)
            return False

        finally:
            # Cerrar el cursor al finalizar
            if 'cursor' in locals() and cursor is not None:
                cursor.close()

    # Devuleve el id del elemento guardado en la columna de la tabla de la base de datos
    def getid(self, table_name, column_name, value_name):

        query = f"SELECT * FROM {table_name} WHERE {column_name} = '{value_name}'"
        try:

            with self.connection.cursor() as cursor:
                cursor.execute(query)
                id = cursor.fetchone()[0]
                return id

        except mysql.connector.Error as error:
            print(error)
            return False

        finally:
            # Cerrar el cursor al finalizar
            cursor.close()

    def export_json_from_table(self, table_name):

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

                # print(f"b.1) Verificando existencia de la fila {item} dentro de la tabla {table_name}")
                [value_exist, existing_values] = verify_existing_value_in_file(cursor, table_name, table_columns, item)

                # print(f"Elemento: {item} {'repetido' if value_exist else 'nuevo'}")

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
            # Crea la tabla
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
