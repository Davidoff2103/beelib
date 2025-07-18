import json
import os
import re

"""
Read configuration file and return parsed data.

Args:
    conf_file (str, optional): Path to the configuration file. If not provided,
        it will read from the environment variable 'CONF_FILE'.

Returns:
    dict: Parsed JSON configuration data with processed authentication tuples.
"""

def read_config(conf_file=None):
    if conf_file:
        conf = json.load(open(conf_file))
    else:
        conf = json.load(open(os.environ['CONF_FILE']))
    for k in [re.match(r'neo4j.*', k).string for k in conf.keys() if re.match(r'neo4j.*', k) is not None]:
        if 'auth' in conf[k]:
            conf[k]['auth'] = tuple(conf[k]['auth'])
    return conf