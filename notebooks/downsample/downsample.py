import sys
sys.path.append('/users/pradap/Documents/Research/Python-Package/scaling/dmagellan')
from dmagellan.core.stringcontainer import StringContainer
from dmagellan.core.tokencontainer import TokenContainer
from dmagellan.core.invertedindex import InvertedIndex
from dmagellan.core.prober import Prober
from dmagellan.core.downsample import get_str_cols
import pandas 
import string
import pandas as pd
#########
str2bytes = lambda x: x if isinstance(x, bytes) else x.encode('utf-8')
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

def probe(objtc, objinvindex, yparam):
    objprobe = Prober()
    objprobe.probe(objtc, objinvindex, yparam)
    return objprobe

def get_lrids(result_list):
    lids = set()
    rids = set()
    for i in range(len(result_list)):
        result = result_list[i]
        lids.update(result.get_lids())
        rids.update(result.get_rids())
    lids = sorted(lids)
    rids = sorted(rids)
    return [lids, rids]

def postprocess(result_list, ltable, rtable, lid, rid):
    lids = set()
    rids = set()
    for i in range(len(result_list)):
        result = result_list[i]
        lids.update(result.get_lids())
        rids.update(result.get_rids())
    lids = sorted(lids)
    rids = sorted(rids)
    if isinstance(ltable, pandas.core.frame.DataFrame):     
        s_ltable = ltable[ltable[lid].isin(lids)]
        s_rtable = rtable[rtable[rid].isin(rids)]
        return (s_ltable, s_rtable)
    else:
        s_ltable = ltable.map_partitions(lambda x: x[x[lid].isin(locs)])
        s_rtable = rtable.map_partitions(lambda x: x[x[rid].isin(locs)])
        return (s_ltable, s_rtable)
        
    

import os
def get_stop_words(path):
    stop_words_set = set()
    with open(path, "rb") as stopwords_file:
        for stop_words in stopwords_file:
            stop_words_set.add(stop_words.rstrip())

    return list(stop_words_set)
    