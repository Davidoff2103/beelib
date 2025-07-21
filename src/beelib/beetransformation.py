import hashlib
import tempfile
from urllib.parse import quote

from neo4j import GraphDatabase
import json
import morph_kgc
import os


def __map_to_ttl__(data, mapping_file):
    """
    Map data to TTL format using a given mapping file.

    Args:
        data (dict): The input data to be mapped.
        mapping_file (str): Path to the mapping file.

    Returns:
        Graph: The resulting RDF graph in TTL format.
    """
    morph_config = "[DataSource1]\nmappings:{mapping_file}\nfile_path: {d_file}"
    with tempfile.NamedTemporaryFile(mode='w', delete=False, dir=".", suffix=".json") as d_file:
        json.dump({k: v for k, v in data.items()}, d_file)
    g_rdflib = morph_kgc.materialize(morph_config.format(mapping_file=mapping_file, d_file=d_file.name))
    os.unlink(d_file.name)
    return g_rdflib

def __transform_to_str__(graph):
    """
    Convert the graph to a string in TTL format.

    Args:
        graph: The RDF graph.

    Returns:
        str: The serialized graph as a string.
    """
    content = graph.serialize(format="ttl")
    content = content.replace('\\\\\"', "\u0026apos;")
    content = content.replace("'", "\u0026apos;")
    return content

def map_and_save(data, mapping_file, config):
    """
    Map data using the provided mapping file and save to Neo4j.

    Args:
        data (dict): Input data to be mapped.
        mapping_file (str): Path to the mapping file.
        config (dict): Configuration for Neo4j connection.

    Returns:
        None
    """
    g = __map_to_ttl__(data, mapping_file)
    save_to_neo4j(g, config)

def map_and_print(data, mapping_file, config):
    """
    Map data using the provided mapping file and print the result.

    Args:
        data (dict): Input data to be mapped.
        mapping_file (str): Path to the mapping file.
        config (dict): Configuration for Neo4j connection.

    Returns:
        None
    """
    g = __map_to_ttl__(data, mapping_file)
    print_graph(g, config)

def save_to_neo4j(g, config):
    """
    Save the RDF graph to Neo4j using the provided configuration.

    Args:
        g: The RDF graph.
        config (dict): Configuration for Neo4j connection.

    Returns:
        None
    """
    content = __transform_to_str__(g)
    neo = GraphDatabase.driver(**config['neo4j'])
    with neo.session() as s:
        response = s.run(f\"\"\"CALL n10s.rdf.import.inline('{content}','Turtle')\"\"\")
        print(response.single())

def print_graph(g, config):
    """
    Print the RDF graph to a file or standard output.

    Args:
        g: The RDF graph.
        config (dict): Configuration for printing options.

    Returns:
        None
    """
    content = __transform_to_str__(g)
    if 'print_file' in config and config['print_file']:
        with open(config['print_file'], "w") as f:
            f.write(content)
    else:
        print(content)

def create_hash(uri):
    """
    Create a hash of the URI using SHA-256.

    Args:
        uri (str): The URI to be hashed.

    Returns:
        str: Hexadecimal representation of the SHA-256 hash.
    """
    uri = quote(uri, safe=':/#')
    uri = uri.encode()
    m = hashlib.sha256(uri)
    return m.hexdigest()