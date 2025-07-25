import time
from datetime import datetime, timezone

import isodate
from pydruid.db import connect
import pandas as pd


def run_druid_query(druid_conf, query):
    """
    Executes a Druid SQL query and returns the result as a list of dictionaries.

    Args:
        druid_conf (dict): Configuration for connecting to Druid (e.g. host, port, path).
        query (str): SQL query to execute.

    Returns:
        List[dict]: Query results with each row represented as a dictionary.
    """
    druid = connect(**druid_conf)
    cursor = druid.cursor()
    cursor.execute(query)
    df_dic = cursor.fetchall()
    return [item._asdict() for item in df_dic]


def get_timeseries_from_druid(d_hash, druid_connection, druid_datasource, ts_ini, ts_end):
    """
    Retrieves a harmonized timeseries from Druid based on a hash and time window.

    Args:
        d_hash (str): Hash value identifying the timeseries.
        druid_connection (dict): Connection configuration for Druid.
        druid_datasource (str): Name of the Druid datasource.
        ts_ini (datetime): Start time for the query window.
        ts_end (datetime): End time for the query window.

    Returns:
        pd.DataFrame: Timeseries data with 'start' as the datetime index.
    """
    druid_query = """
    SELECT
        t1."__time" AS "start",
        MILLIS_TO_TIMESTAMP(CAST(JSON_VALUE(PARSE_JSON(TO_JSON_STRING(t1."end")), '$.rhs') AS BIGINT) * 1000) AS "end",
        "isReal",
        JSON_VALUE(PARSE_JSON(TO_JSON_STRING(t1."value")), '$.rhs') AS "value"
    FROM "{datasource}"  t1
    JOIN (
        SELECT "__time", MAX(MILLIS_TO_TIMESTAMP(CAST("ingestion_time" AS BIGINT) )) AS "ingestion_time" 
        FROM "{datasource}"
            WHERE "hash"='{hash}'
            AND "__time" > TIMESTAMP '{ts_ini}'
            AND "__time" < TIMESTAMP '{ts_end}'
        GROUP BY "__time" 
    ) t2
    ON t1.__time = t2.__time AND MILLIS_TO_TIMESTAMP(CAST(t1."ingestion_time" AS BIGINT)) = t2.ingestion_time
    WHERE t1."hash"='{hash}'
    AND t1."__time"> TIMESTAMP '{ts_ini}'
    AND t1."__time" < TIMESTAMP '{ts_end}'
    """
    data = run_druid_query(
        druid_connection,
        query=druid_query.format(
            datasource=druid_datasource,
            hash=d_hash,
            ts_ini=ts_ini.strftime('%Y-%m-%d %H:%M:%S'),
            ts_end=ts_end.strftime('%Y-%m-%d %H:%M:%S')
        )
    )
    if not data:
        return pd.DataFrame()

    df_data = pd.json_normalize(data)
    df_data = df_data.set_index("start")
    df_data.index = pd.to_datetime(df_data.index)
    return df_data


def harmonize_for_druid(data, timestamp_key, value_key, hash_key, property_key, is_real, freq):
    """
    Prepares a timeseries datapoint for ingestion into Druid.

    Args:
        data (dict): Dictionary with the timeseries data.
        timestamp_key (str): Key for the timestamp field.
        value_key (str): Key for the value field.
        hash_key (str): Key for the hash identifier.
        property_key (str): Key for the property name.
        is_real (bool): Whether the value is real or estimated.
        freq (str): ISO8601 duration string indicating the frequency (e.g., 'PT1H').

    Returns:
        dict: Harmonized data formatted for ingestion into Druid.
    """
    to_save = {
        "start": int(data[timestamp_key].timestamp()),
        "end": int((data[timestamp_key] + isodate.parse_duration(freq)).timestamp() - 1),
        "value": data[value_key],
        "isReal": is_real,
        "hash": data[hash_key],
        "property": data[property_key]
    }
    return to_save


def check_all_ingested(check, druid_connection, druid_datasource):
    """
    Polls Druid to confirm that a specific datapoint has been ingested.

    Args:
        check (dict): Harmonized datapoint to verify.
        druid_connection (dict): Druid connection config.
        druid_datasource (str): Druid datasource name.

    Raises:
        Exception: If the expected data is not found in Druid within 30 seconds.
    """
    if not check:
        return

    # Convert UNIX timestamp to UTC datetime string in ISO format
    utc_dt = datetime.fromtimestamp(check['start'], tz=timezone.utc)
    start = utc_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    druid_query = f"""
    SELECT
        TIMESTAMP_TO_MILLIS(t1."__time") / 1000 AS "start",
        CAST(JSON_VALUE(PARSE_JSON(TO_JSON_STRING(t1."end")), '$.rhs') AS BIGINT) AS "end",
        "isReal",
        CAST(JSON_VALUE(PARSE_JSON(TO_JSON_STRING(t1."value")), '$.rhs') AS DOUBLE) AS "value",
        "hash",
        "property"
    FROM "{druid_datasource}" t1
    JOIN (
        SELECT "__time", MAX(MILLIS_TO_TIMESTAMP(CAST("ingestion_time" AS BIGINT))) AS "ingestion_time"
        FROM "{druid_datasource}" 
        WHERE "hash"='{check["hash"]}' AND "__time"= '{start}' 
        GROUP BY "__time" 
    ) t2
    ON t1.__time = t2.__time AND MILLIS_TO_TIMESTAMP(CAST(t1."ingestion_time" AS BIGINT)) = t2.ingestion_time
    WHERE t1."hash"='{check["hash"]}' AND t1."__time"= '{start}'
    """

    timeout_c = time.time()
    print('Checking all ingestion')
    while time.time() < timeout_c + 30:
        data = run_druid_query(druid_connection, query=druid_query)
        if data:
            if data[0] == check:
                print('Data has been ingested')
                return
        time.sleep(1)

    raise Exception("Timeout exceeded while waiting for data ingestion")
