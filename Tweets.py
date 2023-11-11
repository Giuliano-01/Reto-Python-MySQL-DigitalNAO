from Connection import *
from InteractDatabase import *

##ConnectionSQL Class
new_connection = ConnectionMySQL("localhost", "root", "test")
##Tweets connection
tweet_connection = new_connection.connect_database()
##InteractionSQL Class
interactions_db = InteractMySQL(tweet_connection)

interactions_db.update_table_database_from_json("./tweets_2.json", "Tweets")

#new_connection.update_tables_database_from_json("./tweets_2.json")

