import sys
sys.path.append('/Users/pradap/Documents/Research/Python-Package/scaling/dmagellan')
from dmagellan.core.stringcontainer import StringContainer
import string

import time

def get_str_cols(dataframe):
    return dataframe.columns[dataframe.dtypes == 'object']

def preprocess_table(dataframe):
    str_cols = get_str_cols(dataframe)
    projected_df = dataframe[str_cols]
    concat_strings = []
    str2bytes = lambda x: x if isinstance(x, bytes) else x.encode('utf-8')
    str_container_obj = StringContainer()
    for row in projected_df.itertuples(name=None):
        idx = row[0]
        joined_row = ' '.join(row[1:])
        joined_row = joined_row.translate(None, string.punctuation)
        concat_strings.append(joined_row.lower())
        str_container_obj.push_back(idx, str2bytes(joined_row.lower()))

    return str_container_obj

import py_entitymatching as em
A = em.load_dataset('person_table_A')
print('Calling preprocess_table')
objs = preprocess_table(A)
print('preprocess_table done !!')
print(objs.get_index(0))
