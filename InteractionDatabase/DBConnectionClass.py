import time
import mysql.connector
from mysql.connector import errorcode
from InteractionDatabase.Utils import *


class ConnectionMySQL:

    def __init__(self, host, user, database, password):

        self.host = host
        self.user = user
        self.database = database
        self.password = password

    # Crea la base de datos
    def create_database(self, database):
        try:
            with mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password
            ) as connection:
                print("Connected to MySQL Server version", connection.get_server_info())

                if connection.is_connected():
                    cursor = connection.cursor()

                    # Utilizando formato de cadena f-string
                    cursor.execute(f"CREATE DATABASE {database}")

                    print("Database created\nNew databases list:")
                    cursor.execute("SHOW DATABASES")
                    for x in cursor:
                        print(x)

                    cursor.close()

                return None

        except mysql.connector.Error as error:
            print(f"Error while connecting to MySQL: {error}")
            return None

        finally:
            if connection.is_connected():
                connection.close()
                print("MySQL connection is closed")

    # Conecta a una base de datos y retorna esa conexion
    def connect_database(self, attempts=1, delay=2):
        attempt = 0
        create_database = True

        while attempt < attempts:
            try:
                connection = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
                print("Connected to MySQL Server version", connection.get_server_info())

                return connection

            except mysql.connector.Error as error:

                if attempt == attempts:
                    print(f"Failed to connect, exiting without a connection. Error: {error}")
                    return None

                print("Connection failed: Retrying...", attempt, "/", attempts - 1)
                if error.errno == errorcode.ER_BAD_DB_ERROR and create_database:
                    print("Database doesn't exist")
                    print("Do you want to create new one?: ")
                    create_database = choose_yesno()
                    if create_database:
                        print("Yes")
                        self.create_database(self.database)
                    else:
                        print("Failed to connect, exiting without a connection")
                        return None

                time.sleep(delay ** attempt)
                attempt += 1

        return None
