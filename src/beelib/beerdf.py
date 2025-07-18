from neo4j import GraphDatabase
from rdflib import graph, RDF
import rdflib

def __get_namespaced_fields__(field, context):
    """
    Obtiene el campo RDF con prefijo de espacio de nombres a partir del nombre de campo en formato 'prefix__property'.

    Args:
        field (str): Campo en formato 'prefix__property'.
        context (dict): Diccionario de prefijos RDF (rdflib.Namespace).

    Returns:
        rdflib.term.URIRef | None: Propiedad RDF construida o None si no es válida.
    """
    split_type = field.split("__")
    if len(split_type) > 2 or len(split_type) < 2:
        return None
    else:
        return context[split_type[0]][split_type[1]]

def create_rdf_from_neo4j(neo4j_graph, context):
    """
    Convierte un grafo de Neo4j en un grafo RDF utilizando un contexto con prefijos RDF.

    Args:
        neo4j_graph (neo4j.graph.Graph): Grafo obtenido desde una consulta Cypher.
        context (neo4j.graph.Graph): Grafo de nodos de definición de prefijos (`_NsPrefDef`).

    Returns:
        rdflib.Graph: Grafo RDF resultante.
    """
    context_ns = {}
    # Construye el diccionario de prefijos desde el contexto
    for c in context.nodes:
        for k, v in c.items():
            prefix_tmp = rdflib.Namespace(v)
            context_ns[k] = prefix_tmp

    g = graph.Graph()

    # Procesa los nodos
    for n in neo4j_graph.nodes:
        # Obtiene el URI del sujeto
        try:
            subject = rdflib.URIRef(n.get("uri"))
        except:
            continue

        # Añade las clases RDF (rdf:type)
        for k in [t for t in n.labels if t != "Resource"]:
            lab = __get_namespaced_fields__(k, context_ns)
            if not lab:
                continue
            g.add((subject, RDF.type, lab))

        # Añade las propiedades de datos
        for k in [p for p in n.keys() if p != "uri" and "@" not in p]:
            lab = __get_namespaced_fields__(k, context_ns)
            if not lab:
                continue
            item = n.get(k)

            if isinstance(item, list):
                for item_val in item:
                    if isinstance(item_val, str):
                        # Procesa literales con posible etiqueta de idioma
                        text = item_val.split("@")[0]
                        try:
                            lang = item_val.split("@")[1]
                            v = rdflib.Literal(text, lang=lang)
                        except:
                            v = rdflib.Literal(text)
                    else:
                        v = rdflib.Literal(item_val)
                    g.add((subject, lab, v))
            else:
                v = rdflib.Literal(item)
                g.add((subject, lab, v))

    # Procesa relaciones
    for r in neo4j_graph.relationships:
        rel_type = __get_namespaced_fields__(r.type, context_ns)
        if not rel_type:
            continue
        try:
            subject_orig = rdflib.URIRef(r.start_node.get('uri'))
            subject_end = rdflib.URIRef(r.end_node.get('uri'))
            g.add((subject_orig, rel_type, subject_end))
        except:
            pass

    # Añade los prefijos al grafo RDF
    for k, v in context_ns.items():
        g.bind(k, v)

    return g

def get_rdf_with_cyper_query(query, connection):
    """
    Ejecuta una consulta Cypher en Neo4j, recupera nodos y contexto RDF, y los convierte en grafo RDF.

    Args:
        query (str): Consulta Cypher principal.
        connection (dict): Configuración de conexión Neo4j (host, auth, etc).

    Returns:
        rdflib.Graph: Grafo RDF construido a partir del resultado de la consulta.
    """
    driver = GraphDatabase.driver(**connection)
    with driver.session() as session:
        users = session.run(query).graph()
        context = session.run(
            """MATCH (n:`_NsPrefDef`) RETURN n"""
        ).graph()
    return create_rdf_from_neo4j(neo4j_graph=users, context=context)

def serialize_with_cyper_query(query, connection, format):
    """
    Ejecuta una consulta Cypher y serializa el grafo RDF resultante en el formato deseado.

    Args:
        query (str): Consulta Cypher a ejecutar.
        connection (dict): Configuración de conexión a Neo4j.
        format (str): Formato de serialización RDF (ej: 'turtle', 'xml', 'json-ld').

    Returns:
        str: RDF serializado como cadena de texto.
    """
    g = get_rdf_with_cyper_query(query, connection)
    return g.serialize(format=format)
