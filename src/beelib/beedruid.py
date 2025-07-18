import time
from datetime import datetime, timezone
import isodate
from pydruid.db import connect
import pandas as pd

"""
Run a Druid query and return results as a list of dictionaries.

Args:
    druid_conf (dict): Druid connection configuration.
    query (str): SQL query to execute.

Returns:
    list: List of dictionaries representing query results.
"""

def run_druid_query(druid_conf, query):
    druid = connect(**druid_conf)
    cursor = druid.cursor()
    cursor.execute(query)
    df_dic = cursor.fetchall()
    return [item._asdict() for item in df_dic]

"""
Fetch time series data from Druid with harmonicized formatting.

Args:
    d_hash (str): Data hash identifier.
    druid_connection (dict): Druid connection configuration.
    druid_datasource (str): Druid datasource name.
    ts_ini (datetime): Start timestamp.
    ts_end (datetime): End timestamp.

Returns:
    pd.DataFrame: Harmonized time series data.
"""

def get_timeseries_from_druid(d_hash, druid_connection, druid_datasource, ts_ini, ts_end):
    druid_query = """SELECT\n        t1.__time AS start,\n        MILLIS_TO_TIMESTAMP(CAST(JSON_VALUE(PARSE_JSON(TO_JSON_STRING(t1.end)) as BIGINT) * 1000) AS TIMESTAMP) AS end,\n        isReal,\n        JSON_VALUE(PARSE_JSON(TO_JSON_STRING(t1.value)), '$.rhs') AS value\n    FROM {datasource} t1\n    JOIN (\n        SELECT __time, MAX(MILLIS_TO_TIMESTAMP(CAST(ingestion_time AS BIGINT))) AS ingestion_time\n        FROM {datasource}\n        WHERE hash='{hash}'\n        AND __time > TIMESTAMP '{ts_ini}'\n        AND __time < TIMESTAMP '{ts_end}'\n        GROUP BY __time\n    ) t2\n    ON t1.__time = t2.__time AND MILLIS_TO (CAST(t1.ingestion_time AS BIGINT)) = t2.ingestion_time\n    WHERE t1.hash='{hash}'\n    AND t1.__time > TIMESTAMP '{ts_ini}'\n    AND t1.__time < TIMESTAMP '{ts_end}'\n"""

    data = run_druid_query(druid_connection,
                           query=druid_query.format(
                            datasource=druid_datasource,
                            hash=d_hash,
                            ts_ini=ts_ini.strftime('%Y-%m-%d %H:%M:%S'),
                            ts_end=ts_end.strftime('%Y-%m-%d %H:%M:%S')
                           ))
    if not data:
        return pd.DataFrame()
    df_data = pd.json_normalize(data)
    df_data = df_data.set_index("start")
    df_data.index = pd.to_datetime(df_data.index)
    return df_data