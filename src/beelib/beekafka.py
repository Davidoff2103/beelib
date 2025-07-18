import json
from kafka import KafkaProducer, KafkaConsumer
from base64 import b64encode, b64decode
import pickle
import sys

# --- Serializadores y deserializadores ---

def __pickle_encoder__(v):
    """
    Codifica un objeto serializándolo con pickle y convirtiéndolo a base64.
    """
    return b64encode(pickle.dumps(v))

def __pickle_decoder__(v):
    """
    Decodifica un objeto de base64 y lo deserializa con pickle.
    """
    return pickle.loads(b64decode(v))

def __json_encoder__(v):
    """
    Codifica un objeto en JSON y lo convierte a bytes UTF-8.
    """
    return json.dumps(v).encode("utf-8")

def __json_decoder__(v):
    """
    Decodifica bytes UTF-8 a JSON.
    """
    return json.loads(v.decode("utf-8"))

def __plain_decoder_encoder(v):
    """
    Codificador/decodificador de paso (sin transformación).
    """
    return v

# --- Kafka Producer ---

def create_kafka_producer(kafka_conf, encoding="JSON", **kwargs):
    """
    Crea un KafkaProducer con el encoder correspondiente.

    Args:
        kafka_conf (dict): Diccionario con 'host' y 'port' para Kafka.
        encoding (str): Formato de codificación: 'PLAIN', 'PICKLE', o 'JSON'.
        **kwargs: Argumentos adicionales para KafkaProducer.

    Returns:
        KafkaProducer: Productor de Kafka configurado.
    """
    if encoding == "PLAIN":
        encoder = __plain_decoder_encoder
    elif encoding == "PICKLE":
        encoder = __pickle_encoder__
    elif encoding == "JSON":
        encoder = __json_encoder__
    else:
        raise NotImplementedError("Unknown encoding")

    servers = [f"{kafka_conf['host']}:{kafka_conf['port']}"]
    return KafkaProducer(bootstrap_servers=servers, value_serializer=encoder, **kwargs)

# --- Kafka Consumer ---

def create_kafka_consumer(kafka_conf, encoding="JSON", **kwargs):
    """
    Crea un KafkaConsumer con el decoder correspondiente.

    Args:
        kafka_conf (dict): Diccionario con 'host' y 'port' para Kafka.
        encoding (str): Formato de codificación: 'PLAIN', 'PICKLE', o 'JSON'.
        **kwargs: Argumentos adicionales para KafkaConsumer.

    Returns:
        KafkaConsumer: Consumidor de Kafka configurado.
    """
    if encoding == "PLAIN":
        decoder = __plain_decoder_encoder()
    elif encoding == "PICKLE":
        decoder = __pickle_decoder__
    elif encoding == "JSON":
        decoder = __json_decoder__
    else:
        raise NotImplementedError("Unknown encoding")

    servers = [f"{kafka_conf['host']}:{kafka_conf['port']}"]
    return KafkaConsumer(bootstrap_servers=servers, value_deserializer=decoder, **kwargs)

# --- Envío de mensajes ---

def send_to_kafka(producer, topic, key, data, **kwargs):
    """
    Envía un mensaje al topic especificado de Kafka.

    Args:
        producer (KafkaProducer): Productor Kafka instanciado.
        topic (str): Nombre del topic al que se envía el mensaje.
        key (str): Clave del mensaje (opcional).
        data (any): Contenido del mensaje.
        **kwargs: Otros campos que se incluirán en el mensaje.

    Nota:
        El mensaje se empaqueta como un diccionario con clave "data" y otros campos adicionales.
    """
    try:
        kafka_message = {
            "data": data
        }
        kafka_message.update(kwargs)
        if key:
            producer.send(topic, key=key.encode('utf-8'), value=kafka_message)
        else:
            producer.send(topic, value=kafka_message)
    except Exception as e:
        print(e, file=sys.stderr)
