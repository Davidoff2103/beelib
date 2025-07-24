import json
from kafka import KafkaProducer, KafkaConsumer
from base64 import b64encode, b64decode
import pickle
import sys


def __pickle_encoder__(v):
    """
    Pickle encoder for Kafka producer.

    Args:
        v: The value to be encoded.

    Returns:
        bytes: Encoded value using base64.
    """
    return b64encode(pickle.dumps(v))

def __pickle_decoder__(v):
    """
    Pickle decoder for Kafka consumer.

    Args:
        v: The encoded value.

    Returns:
        object: Decoded Python object.
    """
    return pickle.loads(b64decode(v))

def __json_encoder__(v):
    """
    JSON encoder for Kafka producer.

    Args:
        v: The value to be encoded.

    Returns:
        bytes: Encoded JSON string.
    """
    return json.dumps(v).encode("utf-8")

def __json_decoder__(v):
    """
    JSON decoder for Kafka consumer.

    Args:
        v: The encoded value.

    Returns:
        dict: Decoded JSON object.
    """
    return json.loads(v.decode("utf-8"))

def __plain_decoder_encoder(v):
    """
    Plain text encoder/decoder for Kafka.

    Args:
        v: The value to be encoded or decoded.

    Returns:
        bytes: Encoded value.
    """
    return v


def create_kafka_producer(kafka_conf, encoding="JSON", **kwargs):
    """
    Create a Kafka producer with specified encoding.

    Args:
        kafka_conf (dict): Kafka connection configuration.
        encoding (str): Encoding type. Can be 'PLAIN', 'PICKLE', or 'JSON'.
        **kwargs: Additional arguments for KafkaProducer.

    Returns:
        KafkaProducer: Configured Kafka producer instance.
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


def create_kafka_consumer(kafka_conf, encoding="JSON", **kwargs):
    """
    Create a Kafka consumer with specified encoding.

    Args:
        kafka_conf (dict): Kafka connection configuration.
        encoding (str): Encoding type. Can be 'PLAIN', 'PICKLE', or 'JSON'.
        **kwargs: Additional arguments for KafkaConsumer.

    Returns:
        KafkaConsumer: Configured Kafka consumer instance.
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


def send_to_kafka(producer, topic, key, data, **kwargs):
    """
    Send data to a Kafka topic.

    Args:
        producer (KafkaProducer): The Kafka producer instance.
        topic (str): The target Kafka topic.
        key (bytes): Optional key for the message.
        data (dict): The payload data.
        **kwargs: Additional arguments for Kafka send operation.

    Returns:
        None
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