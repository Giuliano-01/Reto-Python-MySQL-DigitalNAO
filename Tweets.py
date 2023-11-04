from Connection import *

new_connection = ConnectionMySQL("localhost", "root", "test2")
tweet_connection = new_connection.connect_database()

print(tweet_connection)

if tweet_connection:
    print("Is connected")
else:
    print("Isn't connected")


##import pandas as pd
##tweets = pd.read_json("./tweets_2.json")
##id = tweets["id"]
##texto = tweets["texto"]
##retweets = tweets["retweets"]
##favoritos = tweets["favoritos"]
##
##print(id)