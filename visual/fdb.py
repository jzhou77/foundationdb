import xml.dom.minidom
import pandas as pd

def filter_dict(input_dict, kept_keys, sep='\n'):
    # only keep keys in the "kept_keys", combine all others to "Details"
    # with a string concatenated with "sep"
    other = ''
    d = {}
    for key, value in input_dict.items():
        if key in kept_keys:
            d[key] = value
        else:
            if 'Details' in d:
                d['Details'] += sep + key + ": " + value
            else:
                d['Details'] = key + ": " + value
    return d


def load_trace_file(filename, columns):
    dom = xml.dom.minidom.parse(filename)
    events = dom.getElementsByTagName('Event')
    data = [filter_dict(dict(e.attributes.items()), columns) for e in events]
    return pd.DataFrame(data)

def get_roles(df):
    """df is a dataframe obtained from load_trace_file
    """
    return df['As'].dropna().unique().tolist()

def get_machines(df):
    return df['Machine'].dropna().unique().tolist()

