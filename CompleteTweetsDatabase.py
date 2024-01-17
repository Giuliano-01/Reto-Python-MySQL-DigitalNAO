from Connection import *
from InteractDatabase import *
import pandas as pd

# ConnectionSQL Class
tweet_connection = ConnectionMySQL("localhost", "root", "test", "4321").connect_database()
# InteractionSQL Class
interactions_db = InteractMySQL(tweet_connection)

# Read data
data = pd.read_json("./tweets_2.json")

# Tabla Users
print("------- Creando Users")
# interactions_db.create_insert_data_table_columns_from_data("users", "id", ["usuario", "test"], data)

# Tabla Tweets
print("------- Creando Tweets")
# interactions_db.create_insert_data_table_columns_from_data("tweets", "id", ["texto", "retweets", "favoritos", "fecha"], data)

# Tabla Posts (relación Users con Tweets)
print("------- Creando Posts")
# interactions_db.create_table_with_primary_key("posts", "id")
# interactions_db.create_foreign_keys("posts", [("user_id", "users", "id"),
#                                              ("tweet_id", "tweets", "id")])
#
# print("3_ Obteniendo y construyendo lista de {user_id - tweet_id} a partir de las tablas users y tweets.")
#
# users_list = interactions_db.exportJsonFromTable('users')
# tweets_list = interactions_db.exportJsonFromTable('tweets')
#
# user_tweet_list = []
# for index, row in data.iterrows():
#     usuario = row['usuario']
#     tweet = row['texto']
#     user_id = interactions_db.getid("users", "usuario", usuario)
#     tweet_id = interactions_db.getid("tweets", "texto", tweet)
#     user_tweet_list.append({'user_id': user_id, 'tweet_id': tweet_id})


# print("4_ Insertando datos en la tabla Posts")
# interactions_db.insert_data_table_from_json("posts", user_tweet_list)

# Tabla Hashtags
print("------- Creando Hashtags")

# lista_hashtags = []
# for index, hashtag_list in enumerate(data["hashtags"]):
#
#     for hashtag in hashtag_list:
#         lista_hashtags.append({'hashtag': hashtag})
#
# interactions_db.create_insert_data_table_columns_from_data("hashtags", "id", ["hashtag"], lista_hashtags)

# Tabla HashtagsInPosts (Relacion posts con hashtags)
print("------- Creando HashtagsInPosts")

# interactions_db.create_table_with_primary_key("hashtagsinposts", "id")
# interactions_db.create_foreign_keys("hashtagsinposts", [("post_id", "posts", "id"),
#                                                         ("hashtag_id", "hashtags", "id")])


# print("3_ Obteniendo y construyendo lista de {post_id - hashtag_id} a partir de las tablas posts y hashtags.")
# user_tweet_list = []
# for index, row in data.iterrows():
#     tweet = row['texto']
#     hashtags = row['hashtags']
#     tweet_id = interactions_db.getid("tweets", "texto", tweet)
#     post_id = interactions_db.getid("posts", "tweet_id", tweet_id)
#     for index, hashtag in enumerate(hashtags):
#         hashtag_id = interactions_db.getid("hashtags", "hashtag", hashtag)
#         user_tweet_list.append({'post_id': post_id, 'hashtag_id': hashtag_id})
#
# print("4_ Insertando datos en la tabla HashtagsInPosts")
# interactions_db.insert_data_table_from_json("hashtagsinposts", user_tweet_list)
