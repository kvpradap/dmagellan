import string


import pandas as pd
import numpy as np
from dask import threaded, delayed

from dmagellan.core.tokencontainer import TokenContainer
from dmagellan.core.invertedindex import InvertedIndex
from dmagellan.core.stringcontainer import StringContainer
from dmagellan.core.prober import Prober
from dmagellan.core.utils import get_str_cols, str2bytes



#### helper functions ####
def preprocess_table(dataframe):
    strcols = get_str_cols(dataframe)
    projdf = dataframe[strcols]
    objsc = StringContainer()

    for row in projdf.itertuples(name=None):
        colvalues = row[1:]
        strings = [colvalue.strip() for colvalue in colvalues if not pd.isnull(colvalue)]
        concat_row = ' '.join(strings)
        concat_row = concat_row.translate(None, string.punctuation)
        objsc.push_back(str2bytes(concat_row))
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

def probe(objtc, ids, objinvindex, yparam):
    objprobe = Prober()
    objprobe.probe(objtc, ids, objinvindex, yparam)
    return objprobe

def postprocess(result_list, ltable, rtable):
    lids = set()
    rids = set()
    for i in range(len(result_list)):
        result = result_list[i]
        lids.update(result.get_lids())
        rids.update(result.get_rids())
    lids = sorted(lids)
    rids = sorted(rids)
    return (ltable.iloc[lids], rtable.iloc[rids])
    # return (lids, rids)
#########################



#### single machine ####
def downsample_sm(ltable, rtable, size, y, stopwords=[]):

    lcat_strings = preprocess_table(ltable)
    ltokens = tokenize_strings(lcat_strings, stopwords)
    invindex = build_inv_index(ltokens)

    # rsample = rtable.sample(size, replace=False)
    rsample = rtable.head(size)
    rcat_strings = preprocess_table(rsample)
    rtokens = tokenize_strings(rcat_strings, stopwords)

    probe_rslt = probe(rtokens, range(len(rsample)), invindex, y)

    sampled_tbls = postprocess([probe_rslt], ltable, rsample)
    
    return sampled_tbls
#########################

#### dask ########### ####
def downsample_dk(ltable, rtable, size, y, stopwords=[], nchunks=1, scheduler=threaded.get, compute=True):
    
    lcat_strings = (delayed)(preprocess_table)(ltable)
    ltokens = (delayed)(tokenize_strings)(lcat_strings, stopwords)
    invindex = (delayed)(build_inv_index)(ltokens)

    rsample = rtable.sample(size, replace=False)
    # rsample = rtable.head(size)
    
    rsplitted = np.array_split(rsample, nchunks)
    idsplitted = np.array_split(range(size), nchunks)

    probe_rslts = []
    for i in range(nchunks):
        rcat_strings = (delayed)(preprocess_table)(rsplitted[i])
        rtokens = (delayed)(tokenize_strings)(rcat_strings, stopwords)
        probe_rslt = (delayed)(probe)(rtokens, idsplitted[i], invindex, y)
        probe_rslts.append(probe_rslt)
    
    sampled_tbls = (delayed)(postprocess)(probe_rslts, ltable, rsample)

    if compute==True:
        return sampled_tbls.compute(get=scheduler)
    else:
        return sampled_tbls
#########################


#### dask  debug########### ####
def downsample_dbg(ltable, rtable, size, y, stopwords=[], nchunks=1,
                  scheduler=threaded.get, compute=True):
    lcat_strings = (preprocess_table)(ltable)
    ltokens = (tokenize_strings)(lcat_strings, stopwords)
    invindex =(build_inv_index)(ltokens)

    # rsample = rtable.sample(size, replace=False)
    rsample = rtable.head(size)

    rsplitted = np.array_split(rsample, nchunks)
    idsplitted = np.array_split(range(size), nchunks)

    probe_rslts = []
    for i in range(nchunks):
        rcat_strings = (preprocess_table)(rsplitted[i])
        rtokens = (tokenize_strings)(rcat_strings, stopwords)
        probe_rslt = (probe)(rtokens, idsplitted[i], invindex, y)
        probe_rslts.append(probe_rslt)

    sampled_tbls = (postprocess)(probe_rslts, ltable, rsample)

    # if compute == True:
    #     return sampled_tbls.compute(get=scheduler)
    # else:
    return sampled_tbls

#########################


