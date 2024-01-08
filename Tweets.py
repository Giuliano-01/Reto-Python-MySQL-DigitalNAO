from Connection import *
from InteractDatabase import *
import pandas as pd
import numpy as np

# ConnectionSQL Class
tweet_connection = ConnectionMySQL("localhost", "root", "test", "4321").connect_database()
# InteractionSQL Class
interactions_db = InteractMySQL(tweet_connection)

# Read data
data = pd.read_json("./tweets_2.json")

# Tabla Users
print("------- Creando Users")
# interactions_db.create_insert_data_table_columns_from_dataframe("users", "id", ["usuario", "test"], data)

# Tabla Tweets
print("------- Creando Tweets")
# interactions_db.create_insert_data_table_columns_from_dataframe("tweets", "id", ["texto", "retweets", "favoritos", "fecha"], data)

# Tabla Posts (relación Users con Tweets)
print("------- Creando Posts")
# interactions_db.create_table_with_primary_key("posts", "id")
# interactions_db.create_foreign_keys("posts", [("user_id", "users", "id"),
#                                              ("tweet_id", "tweets", "id")])

# print("3_ Obteniendo y construyendo lista de {user_id - tweet_id} a partir de las tablas users y tweets.")
users_list = interactions_db.exportJsonFromTable('users')
tweets_list = interactions_db.exportJsonFromTable('tweets')
user_tweet_list = []
for index, row in data.iterrows():
    usuario = row['usuario']
    tweet = row['texto']
    user_id = 0
    tweet_id = 0
    for item in users_list:
        if item['usuario'] == usuario:
            user_id = item['id']
    for  item in tweets_list:
        if item['texto'] == tweet:
            tweet_id = item['id']

    user_tweet_list.append({'user_id': user_id, 'tweet_id': tweet_id})

# print("4_ Insertando datos en la tabla Posts")
# interactions_db.insert_data_table_from_json("posts", user_tweet_list)

# Tabla Hashtags
lista_hashtags = []
for index, hashtag_list in enumerate(data["hashtags"]):

    for hashtag in hashtag_list:
        lista_hashtags.append({'hashtag': hashtag, 'id': index + 1})

hashtag_list = interactions_db.exportJsonFromTable('hashtags')

hashtag_tweet_list = []
for index, row in data.iterrows():
    hashtags_in_tweet = row['hashtags']
    tweet = row['texto']
    hashtag_id = 0
    tweet_id = 0
    for item in hashtag_list:
        print(item)
        if item['hashtag'] in [itm for itm in hashtags_in_tweet]:
            hashtag_id = item['id'] #Aca usa la segunda vez que aparece hay que poner un return asi sale apenas lo ve la primera y despues ver como hacer que saltee o algo





# interactions_db.create_insert_data_table_columns_from_data("hashtags", "id", ["hashtag"], lista_hashtags)

# Tabla HashtagsInPosts (Relacion posts con hashtags)
# print("------- Creando HashtagsInPosts")
# print("------- (Relaciona: posts con hashtags)")
#
print("1_ Creando tabla -HashtagsInPosts- con clave primaria -id-")
interactions_db.create_table_with_primary_key("hashtagsinposts", "id")
print("2_ Creando claves foraneas -{post_id, hashtag_id}- relacionadas con {id de tabla posts, id de tabla hashtags}")
interactions_db.create_foreign_keys("posts",
                                    [("post_id", "posts", "id"),
                                     ("hashtag_id", "hashtags", "id")])

print("3_ Obteniendo y construyendo lista de {post_id - hashtag_id} a partir de las tablas posts y hashtags.")
posts_list = interactions_db.getIdFromTable('posts', "id", 'id', data)
hashtag_list_id = interactions_db.getIdFromTable('hashtags', "id", 'id', data)
hashtag_list_name = interactions_db.getIdFromTable('hashtags', "hashtag", 'id', data)

print("A", hashtag_list_name)
post_hashtags_list = []
for index, hashtag in enumerate(lista_hashtags):

    post_hashtags_list.append({'post_id': hashtag['id'], 'hashtag_id': index+1})

print(post_hashtags_list)
# for index, tweet in enumerate(tweets_list):
#    if index < len(users_list):  # Asegurarse de que haya un usuario correspondiente para cada tweet
#        user_id = users_list[index]['id']
#        tweet_id = tweet['id']
#        user_tweet_list.append({'user_id': user_id, 'tweet_id': tweet_id})
#    else:
#        print(f"No hay usuario correspondiente para el tweet con id {tweet['id']}")
#
print("4_ Insertando datos en la tabla HashtagsInPosts")
# interactions_db.insert_data_table_from_json("posts", user_tweet_list)
