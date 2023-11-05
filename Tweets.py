from Connection import *

new_connection = ConnectionMySQL("localhost", "root", "test")
tweet_connection = new_connection.connect_database()
print(tweet_connection)

new_connection.update_tables_database_from_json("./tweets_2.json")

