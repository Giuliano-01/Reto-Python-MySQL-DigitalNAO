import matplotlib.pyplot as plt
import numpy as np
from InteractionDatabase.DBInteractionClass import *
from InteractionDatabase.DBConnectionClass import *

# ConnectionSQL Class
tweet_connection = ConnectionMySQL("localhost", "root", "tweets_db", "4321").connect_database()

# InteractionSQL Class
interactions_db = InteractMySQL(tweet_connection)

# Read data
data = pd.read_json("./tweets_2.json")


def get_retweets_x_hashtag():
    retweets_por_hashtag = {}

    hashtag_list = interactions_db.export_json_from_table('hashtags')
    for hashtag in hashtag_list:
        hashtag_id = hashtag['id']
        post_id = interactions_db.getcolumnvalue_from_other_columnvalue("hashtagsinposts", "hashtag_id", hashtag_id, "post_id")
        for post in post_id:
            tweet_id = interactions_db.getcolumnvalue_from_other_columnvalue("posts", "id", post[0], "tweet_id")
            retweets = interactions_db.getcolumnvalue_from_other_columnvalue("tweets", "id", tweet_id[0][0], "retweets")
            # Si el hashtag ya está en el diccionario, sumamos los retweets
            if hashtag['hashtag'] in retweets_por_hashtag:
                retweets_por_hashtag[hashtag['hashtag']] += retweets[0][0]
            # Si el hashtag no está en el diccionario, lo añadimos con la cantidad actual de retweets
            else:
                retweets_por_hashtag[hashtag['hashtag']] = retweets[0][0]

    return retweets_por_hashtag


def generar_grafico_de_barras(keys_list, values_list, xlabel, ylabel, title):
    plt.figure(figsize=(18, 9))
    bars = plt.barh(keys_list, values_list, color='skyblue')

    # Añadir etiquetas y título
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)

    # Añadir números de retweets al final de las barras
    for bar, frequency in zip(bars, values_list):
        plt.text(bar.get_width(), bar.get_y() + bar.get_height() / 2, str(frequency),
                 ha='left', va='center')

    plt.show()


def generar_grafico_de_dispersion(keys_list, values_list, x, y):

    # Tamaño de los globos basado en la cantidad de retweets
    sizes = values_list

    # Crear el gráfico de dispersión
    plt.figure(figsize=(12, 6))
    scatter = plt.scatter(x, y, s=sizes, alpha=0.5, color='skyblue')

    # Añadir números de retweets en cada globo
    for i, txt in enumerate(keys_list):
        plt.annotate(txt, (x[i], y[i]), ha='center', va='center', fontsize=8, color='black')

    # Ocultar ejes
    plt.axis('off')

    # Añadir título
    plt.title('Globos de Retweets por Hashtag Distribuidos al Azar')

    # Mostrar el gráfico

    plt.show()
