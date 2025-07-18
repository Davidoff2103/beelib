import uuid
import happybase
import re

def get_tables(str_filter, hbase_conf):
    """
    Obtiene una lista de tablas de HBase que coinciden con un filtro regex.

    Args:
        str_filter (str): Expresión regular para filtrar nombres de tabla.
        hbase_conf (dict): Configuración de conexión a HBase.

    Returns:
        list[str]: Lista de nombres de tablas que coinciden.
    """
    hbase = happybase.Connection(**hbase_conf)
    return [x.decode() for x in hbase.tables() if re.match(str_filter, x.decode())]

def __get_h_table__(hbase, table_name, cf=None):
    """
    Asegura que la tabla HBase exista y la devuelve. Si no existe, la crea.

    Args:
        hbase (Connection): Conexión activa a HBase.
        table_name (str): Nombre de la tabla.
        cf (dict, optional): Column families a crear si la tabla no existe.

    Returns:
        Table: Objeto tabla de HBase.
    """
    try:
        if not cf:
            cf = {"cf": {}}
        hbase.create_table(table_name, cf)
    except Exception as e:
        if str(e.__class__) == "<class 'Hbase_thrift.AlreadyExists'>":
            pass  # La tabla ya existe
        else:
            print(e)  # Otra excepción
    return hbase.table(table_name)

def save_to_hbase(documents, h_table_name, hbase_conf, cf_mapping, row_fields=None, batch_size=1000):
    """
    Guarda una lista de documentos en HBase.

    Args:
        documents (list[dict]): Documentos a guardar.
        h_table_name (str): Nombre de la tabla en HBase.
        hbase_conf (dict): Configuración de conexión a HBase.
        cf_mapping (list[tuple]): Lista de tuplas (column_family, fields) para mapear columnas.
                                  Si `fields` es "all", se insertan todos los campos.
        row_fields (list[str], optional): Lista de campos para construir la clave de fila. Si no se da, se autogenera.
        batch_size (int): Tamaño de los lotes para escritura.
    """
    hbase = happybase.Connection(**hbase_conf)
    table = __get_h_table__(hbase, h_table_name, {cf: {} for cf, _ in cf_mapping})
    h_batch = table.batch(batch_size=batch_size)

    row_auto = 0  # Contador para claves autogeneradas
    uid = uuid.uuid4()  # UUID base para claves

    for d in documents:
        d_ = d.copy()
        # Clave de fila
        if not row_fields:
            row = f"{uid}~{row_auto}"
            row_auto += 1
        else:
            row = "~".join([str(d_.pop(f)) if f in d_ else "" for f in row_fields])

        values = {}
        # Mapear columnas a families
        for cf, fields in cf_mapping:
            if fields == "all":
                for c, v in d_.items():
                    values["{cf}:{c}".format(cf=cf, c=c)] = str(v)
            elif isinstance(fields, list):
                for c in fields:
                    if c in d_:
                        values["{cf}:{c}".format(cf=cf, c=c)] = str(d_[c])
            else:
                raise Exception("Column mapping must be a list of fields or 'all'")
        
        h_batch.put(str(row), values)
    
    h_batch.send()  # Enviar el lote

def get_hbase_data_batch(hbase_conf, hbase_table, row_start=None, row_stop=None, row_prefix=None, columns=None,
                         _filter=None, timestamp=None, include_timestamp=False, batch_size=100000,
                         scan_batching=None, limit=None, sorted_columns=False, reverse=False):
    """
    Recupera datos en lotes desde una tabla HBase mediante escaneo (`scan`).

    Usa `yield` para devolver datos por lotes, útil para grandes volúmenes.

    Args:
        hbase_conf (dict): Configuración de conexión a HBase.
        hbase_table (str): Nombre de la tabla HBase.
        row_start (str, optional): Fila de inicio del escaneo.
        row_stop (str, optional): Fila de fin del escaneo.
        row_prefix (str, optional): Prefijo común para restringir filas.
        columns (list[str], optional): Columnas a devolver (ej: ['cf1:col1']).
        _filter (str, optional): Filtro HBase en formato string.
        timestamp (int, optional): Leer solo versiones con esta marca de tiempo.
        include_timestamp (bool): Si se incluyen los timestamps en el resultado.
        batch_size (int): Tamaño del lote de escaneo.
        scan_batching (int, optional): Tamaño de lote interno para el escaneo.
        limit (int, optional): Número máximo de registros a recuperar.
        sorted_columns (bool): Si las columnas están ordenadas.
        reverse (bool): Si el escaneo debe ir en orden inverso.

    Yields:
        list[tuple]: Lista de tuplas (row_key, data_dict) por cada lote escaneado.
    """
    # Si se da un prefijo, se convierte a rango de filas
    if row_prefix:
        row_start = row_prefix
        row_stop = row_prefix[:-1] + chr(ord(row_prefix[-1]) + 1)

    # Inicializa el límite del primer lote
    if limit:
        current_limit = min(limit, batch_size)
    else:
        current_limit = batch_size

    current_register = 0  # Contador de registros

    while True:
        hbase = happybase.Connection(**hbase_conf)
        table = hbase.table(hbase_table)

        # Ejecuta el escaneo
        data = list(table.scan(
            row_start=row_start,
            row_stop=row_stop,
            columns=columns,
            filter=_filter,
            timestamp=timestamp,
            include_timestamp=include_timestamp,
            batch_size=batch_size,
            scan_batching=scan_batching,
            limit=current_limit,
            sorted_columns=sorted_columns,
            reverse=reverse
        ))

        if not data:
            break

        yield data  # Devuelve el lote

        if len(data) <= 1:
            break

        # Prepara el siguiente row_start para continuar después del último
        last_record = data[-1][0].decode()
        current_register += len(data)

        if limit:
            if current_register >= limit:
                break
            else:
                current_limit = min(batch_size, limit - current_register)

        row_start = last_record[:-1] + chr(ord(last_record[-1]) + 1)
