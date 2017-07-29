import string

import pandas as pd
from dask import threaded, delayed
from dmagellan.sampler.downsample.dsprober import DownSampleProber

from dmagellan.utils.cy_utils.stringcontainer import StringContainer
from dmagellan.utils.py_utils.utils import get_str_cols, str2bytes, sample, split_df, \
    tokenize_strings_wsp, build_inv_index


#### helper functions ####

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

def probe(objtc, objinvindex, yparam):
    objprobe = DownSampleProber()
    objprobe.probe(objtc, objinvindex, yparam)
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
    return (ltable.loc[lids], rtable.loc[rids])
    # return (lids, rids)
#########################



#### single machine ####
def downsample_sm(ltable, rtable, lid, rid, size, y, lstopwords=[], rstopwords=[]):

    lcat_strings = preprocess_table(ltable, lid)
    ltokens = tokenize_strings_wsp(lcat_strings, lstopwords)
    invindex = build_inv_index([ltokens])

    # rsample = rtable.sample(size, replace=False)
    # rsample = rtable.head(size)
    rsample = sample(rtable, size)
    rcat_strings = preprocess_table(rsample, rid)
    rtokens = tokenize_strings_wsp(rcat_strings, rstopwords)

    probe_rslt = probe(rtokens, invindex, y)

    sampled_tbls = postprocess([probe_rslt], ltable, rsample)

    return sampled_tbls
#########################

#### dask ########### ####
def downsample_dk(ltable, rtable, lid, rid, size, y, lstopwords=[], rstopwords=[], nlchunks=1, nrchunks=1, scheduler=threaded.get, compute=True):


    ltokens = []
    lsplitted = delayed(split_df)(ltable, nlchunks)
    for i in range(nlchunks):
        lcat_strings = (delayed)(preprocess_table)(lsplitted[i], lid)
        tokens = (delayed)(tokenize_strings_wsp)(lcat_strings, lstopwords)
        ltokens.append(tokens)

    invindex = (delayed)(build_inv_index)(ltokens)

    #rsample = rtable.sample(size, replace=False)
    # rsample = rtable.head(size)

    #rsplitted = np.array_split(rsample, nchunks)

    rsample = delayed(sample)(rtable, size)
    rsplitted = delayed(split_df)(rsample, nrchunks)
    probe_rslts = []
    for i in range(nrchunks):
        rcat_strings = (delayed)(preprocess_table)(rsplitted[i], rid)
        rtokens = (delayed)(tokenize_strings_wsp)(rcat_strings, rstopwords)
        probe_rslt = (delayed)(probe)(rtokens, invindex, y)
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
    ltokens = []
    for i in range(nlchunks):
        lcat_strings = preprocess_table(ltable)
        tokens = tokenize_strings_wsp(lcat_strings, stopwords)
        ltokens.append(tokens)

    invindex = build_inv_index(ltokens)

    rsample = sample(rtable, size)
    rsplitted = split_df(rsample, nrchunks)
    probe_rslts = []
    for i in range(nrchunks):
        rcat_strings = preprocess_table(rsplitted[i])
        rtokens = tokenize_strings_wsp(rcat_strings, stopwords)
        probe_rslt = probe(rtokens, invindex, y)
        probe_rslts.append(probe_rslt)

    sampled_tbls = postprocess(probe_rslts, ltable, rsample)

    return sampled_tbls

#########################
