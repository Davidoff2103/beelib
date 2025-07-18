import time
from datetime import datetime, timezone
import influxdb_client
import isodate
import pandas as pd

def connect_influx(influx_connection):
    """
    Establece una conexión con InfluxDB utilizando la configuración proporcionada.

    Args:
        influx_connection (dict): Diccionario con claves 'url', 'org' y 'token' bajo 'connection'.

    Returns:
        influxdb_client.InfluxDBClient: Cliente de conexión a InfluxDB.
    """
    client = influxdb_client.InfluxDBClient(
        url=influx_connection['connection']['url'],
        org=influx_connection['connection']['org'],
        token=influx_connection['connection']['token'],
        timeout=60000  # Timeout de 60 segundos
    )
    return client

def run_query(influx_connection, query):
    """
    Ejecuta una consulta InfluxQL o Flux en InfluxDB y devuelve el resultado como DataFrame.

    Args:
        influx_connection (dict): Configuración de conexión a InfluxDB.
        query (str): Consulta Flux a ejecutar.

    Returns:
        pd.DataFrame: Resultado de la consulta.
    """
    client = connect_influx(influx_connection)
    query_api = client.query_api()
    return query_api.query_data_frame(query)

def get_timeseries_by_hash(d_hash, freq, influx_connection, ts_ini, ts_end):
    """
    Recupera una serie temporal de InfluxDB filtrada por 'hash' y un rango temporal dado.

    Args:
        d_hash (str): Identificador único (hash) de la serie.
        freq (str): Frecuencia de agregación en formato ISO 8601 (ej. 'PT1H').
        influx_connection (dict): Configuración de conexión a InfluxDB (bucket, measurement, etc.).
        ts_ini (datetime): Tiempo de inicio.
        ts_end (datetime): Tiempo de fin.

    Returns:
        pd.DataFrame: DataFrame con índice 'start' y columnas ['end', 'isReal', 'value'].
    """
    aggregation_window = int(isodate.parse_duration(freq).total_seconds() * 10**9)  # nanosegundos
    start = int(ts_ini.timestamp()) * 10**9  # convertir a nanosegundos
    end = int(ts_end.timestamp()) * 10**9

    # Construcción de la consulta en Flux
    query = f"""
        from(bucket: "{influx_connection['bucket']}")
        |> range(start: time(v:{start}), stop: time(v:{int(end)}))
        |> filter(fn: (r) => r["_measurement"] == "{influx_connection['measurement']}")
        |> filter(fn: (r) => r["hash"] == "{d_hash}")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> filter(fn: (r) => r["is_null"]==0.0)
        |> keep(columns: ["_time", "value", "end", "isReal"])
    """

    client = connect_influx(influx_connection)
    query_api = client.query_api()
    df = query_api.query_data_frame(query)

    if df.empty:
        return pd.DataFrame()

    # Convertir 'end' de epoch seconds a datetime con timezone UTC
    df['end'] = pd.to_datetime(df['end'], unit="s").dt.tz_localize("UTC")

    # Renombrar columna _time a start y establecer como índice
    df.rename(columns={"_time": "start"}, inplace=True)
    df = df[["start", "end", "isReal", "value"]]
    df.set_index("start", inplace=True)

    return df
