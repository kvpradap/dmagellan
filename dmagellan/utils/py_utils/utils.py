import string

from chest import Chest
import dask
from dask.diagnostics import ProgressBar
import pandas as pd
import numpy as np
import six
import logging




from dmagellan.tokenizer.whitespacetokenizer import WhiteSpaceTokenizer
from dmagellan.utils.cy_utils.invertedindex import InvertedIndex
from dmagellan.utils.cy_utils.stringcontainer import StringContainer
from dmagellan.utils.cy_utils.tokencontainer import TokenContainer
logger = logging.getLogger(__name__)

def get_proj_cols(idcol, attr, out):
    ocols = [idcol, attr]
    if out != None:
        out = [c for c in out if c not in ocols]
        ocols.extend(out)
    return ocols


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


def tokenize_strings_wsp(objsc, stopwords):
    n = objsc.size()
    objtc = TokenContainer()
    objtok = WhiteSpaceTokenizer(True, stopwords)
    objtc.tokenize(objsc, objtok)
    return objtc


def tokenize_strings(objsc, tokenizer):
    objtc = TokenContainer()
    objtc.tokenize(objsc, tokenizer)
    return objtc


def build_inv_index(objtc):
    inv_obj = InvertedIndex()
    inv_obj.build_inv_index(objtc)
    return inv_obj


def sample(df, size):
    return df.head(size)
    # return df.sample(size, replace=False)


def split_df(df, nchunks):
    sample_splitted = np.array_split(df, nchunks)
    return sample_splitted

def lsplit_df(df, nchunks):
    sample_splitted = np.array_split(df, nchunks)
    return sample_splitted

def rsplit_df(df, nchunks):
    sample_splitted = np.array_split(df, nchunks)
    return sample_splitted

def candsplit_df(df, nchunks):
    sample_splitted = np.array_split(df, nchunks)
    return sample_splitted



def proj_df(df, cols):
    return df[cols]

def lproj_df(df, cols):
    return df[cols]

def rproj_df(df, cols):
    return df[cols]

def candproj_df(df, cols):
    return df[cols]



def rename_cols(df, cols):
    df.columns = cols
    return df


def add_attributes(candset, ltbl, rtbl, fk_ltable, fk_rtable, lkey, rkey,
                   lout=None, rout=None, l_prefix='l_', r_prefix='r_'):
    index = candset.index
    if lout != None:
        colnames = [l_prefix + c for c in lout]
        # ltbl = ltbl.set_index(index, drop=True)
        ldf = create_proj_df(ltbl, lkey, candset[fk_ltable], lout, colnames)

    if rout != None:
        colnames = [r_prefix + c for c in rout]
        # ltbl = ltbl.set_index(index, drop=True)
        rdf = create_proj_df(rtbl, rkey, candset[fk_rtable], rout, colnames)

    if lout != None:
        ldf.set_index(index, inplace=True, drop=True)
        candset = pd.concat([candset, ldf], axis=1)
    if rout != None:
        rdf.set_index(index, inplace=True, drop=True)
        candset = pd.concat([candset, rdf], axis=1)
    candset.set_index(index, inplace=True, drop=True)
    return candset


def create_proj_df(df, key, vals, attrs, colnames):
    df = df.set_index(key, drop=False)
    df = df.ix[vals, attrs]
    df.reset_index(drop=True, inplace=True)
    df.columns = colnames
    return df


def concat_df(dfs):
    dfs = [x for x in dfs if isinstance(x, pd.DataFrame)]
    res = pd.concat(dfs, ignore_index=True)
    return res


def add_id(df):
    # print('Inside add_id')
    if len(df):
        df.insert(0, '_id', range(len(df)))
    return df


def exec_dag(dag, num_workers=None, cache_size=1e9, scheduler=dask.threaded.get,
             show_progress=False):
    cache = Chest(available_memory=cache_size)
    if show_progress:
        with ProgressBar(), dask.set_options(cache=cache,
                                             num_workers=num_workers):
            result = dag.compute(get=scheduler)
    else:
        with cache:
            result = dag.compute(get=scheduler, num_workers=num_workers)
    return result

# def map_partitions(x, func, *args, **kwargs):
#    out = []
#    for i in xrange(len(x)):
#        res = delayed(func)(x[i], args, **kwargs)
#        out.append(res)
#    return out

# find list difference
def list_diff(a_list, b_list):
    b_set = list_drop_duplicates(b_list)
    return [a for a in a_list if a not in b_set]

def list_drop_duplicates(lst):
    a = []
    for i in lst:
        if i not in a:
            a.append(i)
    return a




def check_attrs_present(table, attrs):

    if not isinstance(table, pd.DataFrame):
        # logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    if attrs is None:
        logger.warning('Input attr. list is null')
        return False

    if isinstance(attrs, list) is False:
        attrs = [attrs]
    status = are_all_attrs_in_df(table, attrs)
    return status

def are_all_attrs_in_df(df, col_names):

    if not isinstance(df, pd.DataFrame):
        # logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    if col_names is None:
        logger.warning('Input col_names is null')
        return False

    df_columns_names = list(df.columns)
    for c in col_names:
        if c not in df_columns_names:
            # if verbose:
            #     logger.warning('Column name (' +c+ ') is not present in dataframe')
            return False
    return True

def get_ts():
    """
    This is a helper function, to generate a random string based on current
    time.
    """
    t = int(round(time.time() * 1e10))
    # Return the random string.
    return str(t)[::-1]