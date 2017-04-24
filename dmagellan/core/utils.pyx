import string

import pandas as pd
import numpy as np
from dask import threaded, delayed

from dmagellan.core.tokencontainer import TokenContainer
from dmagellan.core.invertedindex import InvertedIndex
from dmagellan.core.stringcontainer import StringContainer

def get_str_cols(dataframe):
    return dataframe.columns[dataframe.dtypes == 'object']

def str2bytes(x):
    if isinstance(x, bytes):
        return x
    else:
        return x.encode('utf-8')
def get_stop_words(path):
    stop_words_set = set()
    with open(path, "rb") as stopwords_file:
        for stop_words in stopwords_file:
            stop_words_set.add(stop_words.rstrip())

    return list(stop_words_set)

def preprocess_table(dataframe, idcol):
    strcols = list(get_str_cols(dataframe))
    strcols.append(idcol)
    projdf = dataframe[strcols]
    objsc = StringContainer()
    for row in projdf.itertuples():
        colvalues = row[1:-1]
        uid = row[-1]
        strings = [colvalue.strip() for colvalue in colvalues if not pd.isnull(colvalue)]
        concat_row = str2bytes(' '.join(strings).lower())
        concat_row = concat_row.translate(None, string.punctuation)
        objsc.push_back(uid, concat_row)
    return objsc
def tokenize_strings(objsc, stopwords):
    n = objsc.size()
    objtc = TokenContainer()
    objtc.tokenize(objsc, stopwords)
    return objtc

def build_inv_index(objtc):
    inv_obj = InvertedIndex()
    inv_obj.build_inv_index(objtc)
    return inv_obj

def sample(df, size):
    return df.sample(size, replace=False)

def splitdf(df, nchunks):
    sample_splitted = np.array_split(df, nchunks)
    return sample_splitted

#def map_partitions(x, func, *args, **kwargs):
#    out = []
#    for i in xrange(len(x)):
#        res = delayed(func)(x[i], args, **kwargs)
#        out.append(res)
#    return out
