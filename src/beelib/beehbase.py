import uuid
import happybase
import re


def get_tables(str_filter, hbase_conf):
    """
    Get tables from HBase that match the filter.

    Args:
        str_filter (str): Filter pattern for table names.
        hbase_conf (dict): Configuration for connecting to HBase.

    Returns:
        list: List of table names matching the filter.
    """
    hbase = happybase.Connection(**hbase_conf)
    return [x.decode() for x in hbase.tables() if re.match(str_filter, x.decode())]


def __get_h_table__(hbase, table_name, cf=None):
    """
    Get or create an HBase table.

    Args:
        hbase (happybase.Connection): HBase connection object.
        table_name (str): Name of the table.
        cf (dict, optional): Column family configuration.

    Returns:
        happybase.Table: HBase table object.
    """
    try:
        if not cf:
            cf = {"cf": {}}
        hbase.create_table(table_name, cf)
    except Exception as e:
        if str(e.__class__) == "\u003cclass 'Hbase_thrift.AlreadyExists'\u003e":
            pass
        else:
            print(e)
    return hbase.table(table_name)


def save_to_hbase(documents, h_table_name, hbase_conf, cf_mapping, row_fields=None, batch_size=1000):
    """
    Save documents to HBase.

    Args:
        documents (list): List of documents to save.
        h_table_name (str): Name of the HBase table.
        hbase_conf (dict): Configuration for connecting to HBase.
        cf_mapping (dict): Mapping of column families and fields.
        row_fields (list, optional): Fields to use for row keys.
        batch_size (int, optional): Number of documents per batch.

    Returns:
        None
    """
    hbase = happybase.Connection(**hbase_conf)
    table = __get_h_table__(hbase, h_table_name, {cf: {} for cf, _ in cf_mapping})
    h_batch = table.batch(batch_size=batch_size)
    row_auto = 0
    uid = uuid.uuid4()
    for d in documents:
        d_ = d.copy()
        if not row_fields:
            row = f"{uid}~{row_auto}"
            row_auto += 1
        else:
            row = "~".join([str(d_.pop(f)) if f in d_ else "" for f in row_fields])
        values = {}
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
    h_batch.send()


def get_hbase_data_batch(hbase_conf, hbase_table, row_start=None, row_stop=None, row_prefix=None, columns=None,
                         _filter=None, timestamp=None, include_timestamp=False, batch_size=100000,
                         scan_batching=None, limit=None, sorted_columns=False, reverse=False):
    """
    Get data from HBase in batches.

    Args:
        hbase_conf (dict): Configuration for connecting to HBase.
        hbase_table (str): Name of the HBase table.
        row_start (str, optional): Start row key.
        row_stop (str, optional): End row key.
        row_prefix (str, optional): Prefix for rows.
        columns (list, optional): Columns to retrieve.
        _filter (str, optional): Filter expression.
        timestamp (int, optional): Timestamp for data.
        include_timestamp (bool, optional): Include timestamp in results.
        batch_size (int, optional): Number of rows per batch.
        scan_batching (bool, optional): Enable scan batching.
        limit (int, optional): Maximum number of rows to return.
        sorted_columns (bool, optional): Sort columns.
        reverse (bool, optional): Reverse the order of results.

    Yields:
        list: Batch of data from HBase.
    """
    if row_prefix:
        row_start = row_prefix
        row_stop = row_prefix[:-1] + chr(ord(row_prefix[-1]) + 1)

    if limit:
        if limit > batch_size:
            current_limit = batch_size
        else:
            current_limit = limit
    else:
        current_limit = batch_size
    current_register = 0
    while True:
        hbase = happybase.Connection(**hbase_conf)
        table = hbase.table(hbase_table)
        data = list(table.scan(row_start=row_start, row_stop=row_stop, columns=columns, filter=_filter,
                               timestamp=timestamp, include_timestamp=include_timestamp, batch_size=batch_size,
                               scan_batching=scan_batching, limit=current_limit, sorted_columns=sorted_columns,
                               reverse=reverse))
        if not data:
            break
        yield data
        if len(data) <= 1:
            break

        last_record = data[-1][0].decode()
        current_register += len(data)

        if limit:
            if current_register >= limit:
                break
            else:
                current_limit = min(batch_size, limit - current_register)
        row_start = last_record[:-1] + chr(ord(last_record[-1]) + 1)