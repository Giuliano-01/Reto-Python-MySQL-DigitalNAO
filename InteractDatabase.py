# pip install mysql-connector-python
import time
import pandas as pd

import mysql.connector
from mysql.connector import errorcode


def choose_yesno():
    print("Yes (y) / No (n):")

    yes = {'yes', 'y', 'ye', ''}
    choice = input().lower()
    if choice in yes:
        return True
    else:
        return False


class InteractMySQL:

    def __init__(self, connection):

        self.connection = connection

    def update_table_database_from_json(self, file, table):

        file_data = pd.read_json(file)
        id = "testid"
        query = f"CREATE TABLE `{table}` ("

        for column_name in file_data:

            if column_name == "id":
                query = query + f"`{id}` int(11) NOT NULL AUTO_INCREMENT,"
                                f"PRIMARY KEY ({id}),"
            query = query + f"`birth_date` date NOT NULL,"
                                 "first_name varchar(14) NOT NULL"
                                 ") ENGINE=InnoDB")

        try:

            print("Creating table: ")
            cursor = self.connection.cursor()
            cursor.execute(query)

        except mysql.connector.Error as error:

            if error.errno == errorcode.ER_TABLE_EXISTS_ERROR:

                print("Table already exists. Skipping...")

            else:

                print("Error: ", error)
                return False
