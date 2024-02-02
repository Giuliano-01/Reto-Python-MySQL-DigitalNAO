import pandas as pd
from datetime import date


# Convierte a datetime las columnas fecha de un dataframe
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


# Devuelve la eleccion del usuario en consola (Yes o No)
def choose_yesno():
    print("Yes (y) / No (n):")

    yes = {'yes', 'y', 'ye', ''}
    choice = input().lower()
    if choice in yes:
        return True
    else:
        return False


# Función para determinar el tipo y tamaño del campo basándonos en el valor de la columna del dataframe
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


# Función para determinar el tipo y tamaño del campo basándonos en el valor de la columna del json
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


# Función que verifica si ya existe una fila con los mismos valores retornando ese valor repetido
def verify_existing_value_in_file(cursor, table_name, table_columns, row):
    existing_query = f"SELECT * FROM {table_name} WHERE "
    # Itera sobre table_columns y usa column para retornar un array de "valor de column = %s" por cada columna
    conditions = [f"{column} = %s" for column in table_columns]
    existing_query += ' AND '.join(conditions)

    # Itera sobre table_columns y usa column para retornar un array de "valor de columna por cada columna
    existing_values = [row[column] for column in table_columns]

    # Remplazo el %s por el valor de la columna y hago la consulta
    cursor.execute(existing_query, tuple(existing_values))

    # Retorno el valor repetido y la lista de valores de la columna
    return [cursor.fetchone(), existing_values]

