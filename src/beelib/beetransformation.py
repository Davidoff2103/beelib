import hashlib
import tempfile
from urllib.parse import quote
from neo4j import GraphDatabase
import json
import morph_kgc
import os

def __map_to_ttl__(data, mapping_file):
    """
    Genera un grafo RDF a partir de datos y un archivo de mapeo R2RML usando morph-kgc.

    Args:
        data (dict): Diccionario de datos fuente.
        mapping_file (str): Ruta al archivo de mapeo R2RML (.ttl).

    Returns:
        rdflib.Graph: Grafo RDF generado.
    """
    morph_config = "[DataSource1]\nmappings:{mapping_file}\nfile_path: {d_file}"
    
    # Escribe los datos en un archivo JSON temporal
    with tempfile.NamedTemporaryFile(mode='w', delete=False, dir=".", suffix=".json") as d_file:
        json.dump({k: v for k, v in data.items()}, d_file)
    
    # Llama a morph-kgc para materializar el grafo RDF
    g_rdflib = morph_kgc.materialize(morph_config.format(mapping_file=mapping_file, d_file=d_file.name))
    
    # Elimina el archivo temporal
    os.unlink(d_file.name)
    
    return g_rdflib

def __transform_to_str__(graph):
    """
    Serializa un grafo RDF en formato Turtle y normaliza comillas.

    Args:
        graph (rdflib.Graph): Grafo RDF.

    Returns:
        str: Grafo serializado en formato Turtle como cadena.
    """
    content = graph.serialize(format="ttl")
    content = content.replace('\\"', "&apos;")
    content = content.replace("'", "&apos;")
    return content

def map_and_save(data, mapping_file, config):
    """
    Mapea los datos a RDF y los guarda en una base de datos Neo4j.

    Args:
        data (dict): Datos fuente.
        mapping_file (str): Ruta al archivo R2RML.
        config (dict): Configuración de conexión a Neo4j.
    """
    g = __map_to_ttl__(data, mapping_file)
    save_to_neo4j(g, config)

def map_and_print(data, mapping_file, config):
    """
    Mapea los datos a RDF y los imprime por pantalla o a un archivo.

    Args:
        data (dict): Datos fuente.
        mapping_file (str): Ruta al archivo R2RML.
        config (dict): Configuración con posible ruta de salida ('print_file').
    """
    g = __map_to_ttl__(data, mapping_file)
    print_graph(g, config)

def save_to_neo4j(g, config):
    """
    Envía un grafo RDF serializado a una base de datos Neo4j usando n10s.

    Args:
        g (rdflib.Graph): Grafo RDF.
        config (dict): Configuración de conexión a Neo4j.
    """
    content = __transform_to_str__(g)
    neo = GraphDatabase.driver(**config['neo4j'])
    with neo.session() as s:
        response = s.run(f"""CALL n10s.rdf.import.inline('{content}','Turtle')""")
        print(response.single())

def print_graph(g, config):
    """
    Imprime el grafo RDF en formato Turtle por pantalla o lo guarda en un archivo.

    Args:
        g (rdflib.Graph): Grafo RDF.
        config (dict): Configuración con posible clave 'print_file' para escribir a disco.
    """
    content = __transform_to_str__(g)
    if 'print_file' in config and config['print_file']:
        with open(config['print_file'], "w") as f:
            f.write(content)
    else:
        print(g.serialize(format="ttl"))

def create_hash(uri):
    """
    Genera un hash SHA-256 a partir de un URI.

    Args:
        uri (str): URI que se quiere codificar.

    Returns:
        str: Hash SHA-256 hexadecimal del URI codificado.
    """
    uri = quote(uri, safe=':/#')  # Escapa caracteres especiales conservando separadores comunes
    uri = uri.encode()
    m = hashlib.sha256(uri)
    return m.hexdigest()
