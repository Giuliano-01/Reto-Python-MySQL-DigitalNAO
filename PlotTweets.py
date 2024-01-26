import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
import random
import numpy as np
from SentimentReq import get_sentiments_list

data = pd.read_json("./tweets_2.json")

# Function para

print(get_sentiments_list())





# Inicializamos un diccionario para almacenar la suma de retweets por hashtag
retweets_por_hashtag = {}

# Iteramos sobre la columna 'hashtags' y 'retweets' de cada fila en el DataFrame 'data'
for hashtags_lista, retweets in zip(data['hashtags'], data['retweets']):
    # Iteramos sobre la lista de hashtags de cada fila
    for hashtag in hashtags_lista:
        # Si el hashtag ya está en el diccionario, sumamos los retweets
        if hashtag in retweets_por_hashtag:
            retweets_por_hashtag[hashtag] += retweets
        # Si el hashtag no está en el diccionario, lo añadimos con la cantidad actual de retweets
        else:
            retweets_por_hashtag[hashtag] = retweets


# Imprimimos la suma de retweets por cada hashtag
for hashtag, total_retweets in retweets_por_hashtag.items():
    print(f"Hashtag: {hashtag}, Total de Retweets: {total_retweets}")

# ----- GRAFICO DE BARRAS ----- #
hashtags = list(retweets_por_hashtag.keys())
frequencies = list(retweets_por_hashtag.values())

plt.figure(figsize=(18, 9))
bars = plt.barh(hashtags, frequencies, color='skyblue')

# Añadir etiquetas y título
plt.xlabel('Frecuencia')
plt.ylabel('Hashtags')
plt.title('Frecuencia de Hashtags')

# Añadir números de retweets al final de las barras
for bar, frequency in zip(bars, frequencies):
    plt.text(bar.get_width(), bar.get_y() + bar.get_height() / 2, str(frequency),
             ha='left', va='center')


# ----- GRAFICO DE DISPERSION ----- #
x = np.random.rand(len(retweets_por_hashtag))
y = np.random.rand(len(retweets_por_hashtag))
# Tamaño de los globos basado en la cantidad de retweets
sizes = list(retweets_por_hashtag.values())

# Crear el gráfico de dispersión
plt.figure(figsize=(12, 6))
scatter = plt.scatter(x, y, s=sizes, alpha=0.5, color='skyblue')

# Añadir números de retweets en cada globo
for i, txt in enumerate(list(retweets_por_hashtag.keys())):
    plt.annotate(txt, (x[i], y[i]), ha='center', va='center', fontsize=8, color='black')

# Ocultar ejes
plt.axis('off')

# Añadir título
plt.title('Globos de Retweets por Hashtag Distribuidos al Azar')

# Mostrar el gráfico
plt.show()