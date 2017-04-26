import string

import numpy as np
import pandas as pd

from dmagellan.core.invertedindex import InvertedIndex
from dmagellan.core.stringcontainer import StringContainer
from dmagellan.core.tokencontainer import TokenContainer


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


def projdf(df, cols):
    return df[cols]


def rename(df, cols):
    df.columns = cols
    return df


def add_attrs(candset, ltbl, rtbl, fk_ltable, fk_rtable, lkey, rkey,
              lout=None, rout=None, l_prefix='l_', r_prefix='r_'):

    if lout != None:
        colnames = [l_prefix + c for c in lout]
        ldf = create_proj_df(ltbl, lkey, candset[fk_ltable], lout, colnames)
        candset = pd.concat([candset, ldf], axis=1, ignore_index=True)
    if rout != None:
        colnames = [r_prefix + c for c in rout]
        rdf = create_proj_df(rtbl, rkey, candset[fk_rtable], rout, colnames)
        candset = pd.concat([candset, rdf], axis=1, ignore_index=True)
    return candset

def create_proj_df(df, key, vals, attrs, colnames):
    tmp = df[df[key].isin(vals)]
    tmp = tmp[attrs]
    tmp.columns = colnames
    return tmp


def concatdf(dfs):
    res = pd.concat(dfs, axis=1, ignore_index=True)
    return res

def addid(df):
    df = df.insert(0, '_id', range(len(df)))
    return df
# def map_partitions(x, func, *args, **kwargs):
#    out = []
#    for i in xrange(len(x)):
#        res = delayed(func)(x[i], args, **kwargs)
#        out.append(res)
#    return out
