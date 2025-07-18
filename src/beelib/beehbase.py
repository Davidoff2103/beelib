import uuid
import happybase
import re

"""
List HBase tables matching a filter pattern.

Args:
    str_filter (str): Regular expression to match table names.
    hbase_conf (dict): HBase connection configuration.

Returns:
    list: List of table names matching the filter.
"""

def get_tables(str_filter, hbase_conf):
    hbase = happybase.Connection(**hbase_conf)
    return [x.decode() for x in hbase.tables() if re.match(str_filter, x.decode())]

"""
Create or retrieve an HBase table with specified column families.

Args:
    hbase (happybase.Connection): HBase connection object.
    table_name (str): Name of the table to create/retrieve.
    cf (dict, optional): Column families configuration. Defaults to {"cf": {}}.

Returns:
    happybase.Table: HBase table object.
"""

def __get_h_table__(hbase, table_name, cf=None):
    try:
        if not cf:
            cf = {"cf": {}}
        hbase.create_table(table_name, cf)
    except Exception as e:
        if str(e.__class__) == "<class 'Hbase_thrift.AlreadyExists'>":
            pass
        else:
            print(e)
    return hbase.table(table_name)

"""
Save documents to HBase with specified column mapping.

Args:
    documents (list): List of document dictionaries to save.
    h_table_name (str): Target HBase table name.
    hbase_conf (dict): HBase connection configuration.
    cf_mapping (list): Column family mapping, e.g., [('cf1', ['field1']), ('cf2', ['field2'])].
    row_fields (list, optional): Fields to use for row keys. Defaults to UUID and auto-incrementing counter.
    batch_size (int, optional): Number of rows per batch. Defaults to 1000.
"""

def save_to_hbase(documents, h_table_name, hbase_conf, cf_mapping, row_fields=None, batch_size=1000):
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
                    values[f"{cf}:{c}".format(cf=cf, c=c)] = str(v)
            elif isinstance(fields, list):
                for c in fields:
                    if c in d_:
                        values[f"{cf}:{c}".format(cf=cf, c=c)] = str(d_[c])
            else:
                raise Exception("Column mapping must be a list of fields or 'all'\")
        h_batch.put(str(row), values)
    h_batch.send()