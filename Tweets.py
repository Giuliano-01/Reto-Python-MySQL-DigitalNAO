from Connection import *
from InteractDatabase import *

import pandas as pd
import numpy as np

##ConnectionSQL Class
new_connection = ConnectionMySQL("localhost", "root", "test", "4321")
##Tweets connection
tweet_connection = new_connection.connect_database()
##InteractionSQL Class
interactions_db = InteractMySQL(tweet_connection)

data = pd.read_json("./tweets_2.json")
# Tabla Users
#interactions_db.create_table_with_primary_key("users", "id")
#interactions_db.create_column("users", "usuario", data)
#insert data
# interactions_db.insert_data_table_from_dataframe("users", data)

# Tabla Tweets
#interactions_db.create_table_with_primary_key("tweets", "id")
#interactions_db.create_column("tweets", "texto", data)
#insert data
#interactions_db.insert_data_table_from_dataframe("tweets", data)

# Tabla Posts (relación Users con Tweets)
interactions_db.create_table_with_primary_key("posts", "id")
interactions_db.create_foreign_keys("posts",
                                    [("user_id", "users", "id"),
                                     ("tweet_id", "tweets", "id")])
#insert data


users_list = interactions_db.getIdFromTable('users', 'usuario', data)
tweets_list = interactions_db.getIdFromTable('tweets', 'texto', data)

user_tweet_list = []

for index, tweet in enumerate(tweets_list):
    if index < len(users_list):  # Asegurarse de que haya un usuario correspondiente para cada tweet
        user_id = users_list[index]['id']
        tweet_id = tweet['id']
        user_tweet_list.append({'user_id': user_id, 'tweet_id': tweet_id})
    else:
        print(f"No hay usuario correspondiente para el tweet con id {tweet['id']}")

interactions_db.insert_data_table_from_json("posts", user_tweet_list)