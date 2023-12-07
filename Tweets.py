from Connection import *
from InteractDatabase import *

import pandas as pd

##ConnectionSQL Class
new_connection = ConnectionMySQL("localhost", "root", "test", "4321")
##Tweets connection
tweet_connection = new_connection.connect_database()
##InteractionSQL Class
interactions_db = InteractMySQL(tweet_connection)


data = pd.read_json("./tweets_2.json")

# interactions_db.create_table_from_data("test", data)

# interactions_db.insert_data_from_json("test", data)


