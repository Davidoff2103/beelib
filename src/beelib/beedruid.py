import time
from datetime import datetime, timezone

import isodate
from pydruid.db import connect
import pandas as pd

def run_druid_query(druid_conf, query):
    """
    Executes a Druid SQL query and returns the result as a list of dictionaries.

    Args:
        druid_conf (dict): Configuration for connecting to the Druid database.
        query (str): SQL query to execute.

    Returns:
        list[dict]: List of rows returned by the query, each as a dictionary.
    """
    druid = connect(**druid_conf)  # Connect to Druid using given configuration
    cursor = druid.cursor()
    cursor.execute(query)  # Execute the SQL query
    df_dic = cursor.fetchall()  # Fetch all result rows
    return [item._asdict() for item in df_dic]  # Convert namedtuples to dicts

def get_timeseries_from_druid(d_hash, druid_connection, druid_datasource, ts_ini, ts_end):
    """
    Retrieves a harmonized timeseries from Druid based on hash and time range.

    Args:
        d_hash (str): Unique identifier (hash) of the timeseries.
        druid_connection (dict): Druid connection configuration.
        druid_datasource (str): Druid datasource name.
        ts_ini (datetime): Start timestamp.
        ts_end (datetime): End timestamp.

    Returns:
        pd.DataFrame: Timeseries as a pandas DataFrame with datetime index.
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
    # Format query with actual values
    data = run_druid_query(druid_connection,
                           query=druid_query.format(
                            datasource=druid_datasource,
                            hash=d_hash,
                            ts_ini=ts_ini.strftime('%Y-%m-%d %H:%M:%S'),
                            ts_end=ts_end.strftime('%Y-%m-%d %H:%M:%S')
                           ))
    if not data:
        return pd.DataFrame()  # Return empty DataFrame if no data
    
    df_data = pd.json_normalize(data)  # Normalize JSON to flat table
    df_data = df_data.set_index("start")  # Use 'start' as index
    df_data.index = pd.to_datetime(df_data.index)  # Convert index to datetime
    return df_data

def harmonize_for_druid(data, timestamp_key, value_key, hash_key, property_key, is_real, freq):
    """
    Converts a single timeseries data point to Druid's expected format.

    Args:
        data (dict): Dictionary containing the timeseries point.
        timestamp_key (str): Key in data for timestamp.
        value_key (str): Key in data for value.
        hash_key (str): Key in data for hash ID.
        property_key (str): Key in data for property name.
        is_real (bool): Whether the value is real or synthetic.
        freq (str): ISO 8601 duration (e.g., "PT1H" for 1 hour).

    Returns:
        dict: Harmonized dictionary ready to be sent to Druid.
    """
    to_save = {
        "start": int(data[timestamp_key].timestamp()),  # Start as Unix timestamp
        "end": int((data[timestamp_key] + isodate.parse_duration(freq)).timestamp() - 1),  # End time minus 1 sec
        "value": data[value_key],
        "isReal": is_real,
        "hash": data[hash_key],
        "property": data[property_key]
    }
    return to_save

def check_all_ingested(check, druid_connection, druid_datasource):
    """
    Checks if a specific record has been ingested into Druid.

    Repeatedly queries Druid for up to 30 seconds until the expected record is found.

    Args:
        check (dict): Dictionary with expected data to verify ingestion.
        druid_connection (dict): Druid connection configuration.
        druid_datasource (str): Druid datasource name.

    Raises:
        Exception: If the record is not found within 30 seconds.
    """
    if not check:
        return  # Skip check if no data provided

    # Convert start time to ISO 8601 format with milliseconds and UTC
    utc_dt = datetime.fromtimestamp(check['start'], tz=timezone.utc)
    start = utc_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    # Query to check for exact match of ingested record
    druid_query = f"""
    SELECT
        TIMESTAMP_TO_MILLIS(t1."__time") / 1000 AS "start",
        CAST(JSON_VALUE(PARSE_JSON(TO_JSON_STRING(t1."end")), '$.rhs') AS BIGINT) AS "end",
        "isReal",
        CAST(JSON_VALUE(PARSE_JSON(TO_JSON_STRING(t1."value")), '$.rhs') AS DOUBLE)AS "value",
        "hash",
        "property"
    FROM "{druid_datasource}"  t1
    JOIN (
        SELECT "__time", MAX(MILLIS_TO_TIMESTAMP(CAST("ingestion_time" AS BIGINT) )) AS "ingestion_time" 
        FROM "{druid_datasource}" 
            WHERE "hash"='{check["hash"]}'
            AND "__time"= '{start}' 
        GROUP BY "__time" 
    ) t2
    ON t1.__time = t2.__time AND MILLIS_TO_TIMESTAMP(CAST(t1."ingestion_time" AS BIGINT)) = t2.ingestion_time
    WHERE t1."hash"='{check["hash"]}' AND t1."__time"= '{start}'
    """

    timeout_c = time.time()  # Start timer
    print('checking all ingestion')
    
    # Loop for up to 30 seconds
    while time.time() < timeout_c + 30:
        data = run_druid_query(druid_connection, query=druid_query)
        if data:
            if data[0] == check:
                print('data has been ingested')
                return
            else:
                continue  # If data doesn't match, continue polling
        time.sleep(1)  # Wait before retrying

    raise Exception("Timeout exceeded")  # Raise if data not ingested in time
