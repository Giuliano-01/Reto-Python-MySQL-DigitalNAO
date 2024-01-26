import requests
from Connection import *
from InteractDatabase import *
import pandas as pd

# ConnectionSQL Class
tweet_connection = ConnectionMySQL("localhost", "root", "test", "4321").connect_database()
# InteractionSQL Class
interactions_db = InteractMySQL(tweet_connection)


def obtainsentiment(text):

    api_url = "https://apis.paralleldots.com/v4/sentiment"

    form_data = {
        "text": text,
        "api_key": "frrFZDmigjDWu2ism4GvE79lrMXiEWSzbJij8c20bRE",
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.post(api_url, data=form_data, headers=headers)

    if response.status_code == 200:
        data = response.json()
        sentimiento_maximo = max(data['sentiment'], key=data['sentiment'].get)
        return sentimiento_maximo
    else:
        print(f"Error en la solicitud: {response.status_code}")
        print(response.text)


def get_sentiments_list_api():

    data = pd.read_json("./tweets_2.json")

    tweet_sentiment_list = []
    for index, row in data.iterrows():

        tweet_texto = row['texto']
        tweet_id = interactions_db.getid("tweets", "texto", tweet_texto)
        tweet_sentiment = obtainsentiment(tweet_texto)
        print(tweet_texto)
        print(tweet_sentiment)
        tweet_sentiment_list.append({'tweet_id': tweet_id, 'tweet_sentiment': tweet_sentiment, 'texto': tweet_texto})

    print(tweet_sentiment_list)


def get_sentiments_list():
    return [
        {'tweet_id': 1, 'tweet_sentiment': 'neutral', 'texto': 'La tecnología de la realidad virtual está revolucionando la forma en que experimentamos los videojuegos. #tecnología #videojuegos'},
        {'tweet_id': 2, 'tweet_sentiment': 'positive', 'texto': 'Apple anuncia el lanzamiento de su nuevo iPhone 15 con cámara mejorada y pantalla OLED de 6,7 pulgadas. #Apple #iPhone15'},
        {'tweet_id': 3, 'tweet_sentiment': 'positive', 'texto': 'Google presenta su nuevo asistente virtual con inteligencia artificial que permite controlar el hogar desde el smartphone. #Google #hogarinteligente'},
        {'tweet_id': 4, 'tweet_sentiment': 'neutral', 'texto': 'La empresa de tecnología SpaceX anuncia el lanzamiento de su primer vuelo comercial a la luna. #SpaceX #vuelolunar'},
        {'tweet_id': 5, 'tweet_sentiment': 'positive', 'texto': 'El nuevo dron de DJI es capaz de grabar video en 8K y tiene un tiempo de vuelo de hasta 45 minutos. #drones #tecnología'},
        {'tweet_id': 6, 'tweet_sentiment': 'positive', 'texto': 'La empresa de tecnología Nvidia lanza su nueva tarjeta gráfica RTX 5080 con 24 GB de memoria y capacidad de ray tracing en tiempo real. #Nvidia #tarjetagráfica'},
        {'tweet_id': 7, 'tweet_sentiment': 'positive', 'texto': 'Nuevo iPhone 13 con pantalla de 120Hz'},
        {'tweet_id': 8, 'tweet_sentiment': 'positive', 'texto': 'La próxima generación de procesadores Intel promete un gran avance en rendimiento'},
        {'tweet_id': 9, 'tweet_sentiment': 'positive', 'texto': 'Samsung presenta su nuevo teléfono plegable'},
        {'tweet_id': 10, 'tweet_sentiment': 'positive', 'texto': 'Apple presenta su nueva línea de MacBook Pro con chip M2'},
        {'tweet_id': 11, 'tweet_sentiment': 'neutral', 'texto': 'Google anuncia nuevas herramientas para desarrolladores de Android'},
        {'tweet_id': 12, 'tweet_sentiment': 'neutral', 'texto': 'Elon Musk anuncia un nuevo lanzamiento de cohetes de SpaceX'},
        {'tweet_id': 13, 'tweet_sentiment': 'positive', 'texto': 'Microsoft lanza una actualización importante para Windows 11'},
        {'tweet_id': 14, 'tweet_sentiment': 'positive', 'texto': 'El gigante tecnológico Apple presenta su nueva línea de productos en un evento virtual. Entre las novedades se encuentran el nuevo iPhone y el Apple Watch Series 8. #AppleEvent #nuevosproductos'},
        {'tweet_id': 15, 'tweet_sentiment': 'positive', 'texto': 'La inteligencia artificial está cada vez más presente en nuestras vidas, ¿pero sabemos realmente cómo funciona? En nuestro último artículo explicamos todo sobre esta tecnología. #inteligenciaartificial #IA'},
        {'tweet_id': 16, 'tweet_sentiment': 'neutral', 'texto': 'La tecnología blockchain está revolucionando la forma en que se realizan las transacciones financieras. Descubre todo lo que necesitas saber sobre esta tecnología en nuestro último artículo. #blockchain #criptomonedas'},
        {'tweet_id': 17, 'tweet_sentiment': 'positive', 'texto': 'Los avances en tecnología permiten crear prótesis cada vez más sofisticadas y precisas. En nuestro último artículo hablamos sobre algunos de los desarrollos más innovadores en este campo. #prótesis #tecnología'},
        {'tweet_id': 18, 'tweet_sentiment': 'positive', 'texto': 'La tecnología de realidad aumentada se está convirtiendo en una herramienta indispensable para los diseñadores de moda. Descubre cómo se está utilizando esta tecnología en nuestro último artículo. #realidadaumentada #moda'},
        {'tweet_id': 19, 'tweet_sentiment': 'neutral', 'texto': 'La tecnología de reconocimiento facial está siendo utilizada en muchos ámbitos, pero ¿es realmente segura? En nuestro último artículo analizamos los riesgos de esta tecnología y cómo se pueden mitigar. #reconocimientofacial #seguridad'},
        {'tweet_id': 20, 'tweet_sentiment': 'positive', 'texto': 'La tecnología 5G está revolucionando la forma en que nos conectamos a internet. En nuestro último artículo te explicamos todo lo que necesitas saber sobre esta nueva tecnología. #tecnología5G #internet'},
        {'tweet_id': 21, 'tweet_sentiment': 'neutral', 'texto': 'La tecnología de la nube ha transformado la forma en que almacenamos y compartimos información. En nuestro último artículo te explicamos todo lo que necesitas saber sobre esta tecnología. #tecnologíadelanube #almacenamiento'},
        {'tweet_id': 22, 'tweet_sentiment': 'positive', 'texto': 'La tecnología de la realidad virtual está transformando la forma en que experimentamos el entretenimiento. En nuestro último artículo te contamos todo lo que necesitas saber sobre esta tecnología. #realidadvirtual #entretenimiento'},
        {'tweet_id': 23, 'tweet_sentiment': 'positive', 'texto': 'La tecnología de los drones está siendo utilizada en muchos ámbitos, desde la entrega de paquetes hasta la vigilancia. En nuestro último artículo te explicamos todo lo que necesitas saber sobre esta tecnología. #drones #tecnología'},
        {'tweet_id': 24, 'tweet_sentiment': 'positive', 'texto': 'La tecnología de la inteligencia artificial está transformando la forma en que las empresas toman decisiones. En nuestro último artículo te contamos todo lo que necesitas saber sobre esta tecnología. #inteligenciaartificial #empresas'},
        {'tweet_id': 25, 'tweet_sentiment': 'neutral', 'texto': 'La tecnología de la biometría está siendo utilizada para mejorar la seguridad en muchos ámbitos. En nuestro último artículo te explicamos todo lo que necesitas saber sobre esta tecnología. #biometría #seguridad'}
    ]
