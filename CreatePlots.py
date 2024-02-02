import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from GenerateSentimentPlots.SentimentReq import *
from InteractionDatabase.DBInteractionClass import *
from InteractionDatabase.DBConnectionClass import *
from GenerateSentimentPlots.PlotsInformation import *

# ConnectionSQL Class
tweet_connection = ConnectionMySQL("localhost", "root", "tweets_db", "4321").connect_database()

# InteractionSQL Class
interactions_db = InteractMySQL(tweet_connection)

# Read data
data = pd.read_json("./tweets_2.json")



# Inicializamos un diccionario para almacenar la suma de retweets por hashtag
retweets_por_hashtag = get_retweets_x_hashtag()

# ----- GRAFICO DE BARRAS ----- #

hashtags = list(retweets_por_hashtag.keys())
frequencies = list(retweets_por_hashtag.values())

# generar_grafico_de_barras(hashtags, frequencies, 'Frecuencia', 'Hashtags', 'Frecuencia de Hashtags')

# ----- GRAFICO DE DISPERSION ----- #
x = np.random.rand(len(retweets_por_hashtag))
y = np.random.rand(len(retweets_por_hashtag))

# generar_grafico_de_dispersion(hashtags, frequencies, x, y)


# sentiments list plot
sentiment_list = get_sentiments_list()

tweet_sentiments = [column['tweet_sentiment'] for column in sentiment_list]
neutral = 0
positive = 0
negative = 0
for sentiment in tweet_sentiments:
    if sentiment == 'neutral':
        neutral = neutral+1
    if sentiment == 'positive':
        positive = positive+1
    if sentiment == 'negative':
        negative = negative+1

sentiments_list = {'neutral': neutral, 'positive': positive, 'negative': negative}

sentiments = list(sentiments_list.keys())
sentiments_freq = list(sentiments_list.values())

generar_grafico_de_barras(sentiments, sentiments_freq, 'Frecuencia', 'Sentimientos', 'Frecuencia de Sentimientos de tweets')
