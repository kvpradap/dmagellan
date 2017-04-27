import string
import pandas as pd
from functools import partial

from dask import threaded, delayed
from dmagellan.core.stringcontainer import StringContainer
from dmagellan.core.overlapprober import OverlapProber
from dmagellan.core.tokenizer import Tokenizer

from dmagellan.core.utils import str2bytes, splitdf, build_inv_index

##### helper functions ######
def remove_stopwords(tokens, stopwords):
    otokens = []
    for token in tokens:
        if not stopwords.has_key(token):
            otokens.append(token)
    return otokens
def process_col(column, stopwords):
    column = column.str.translate(None, string.punctuation)
    column = column.str.lower()
    if stopwords != None:
        stopword_dict = dict(zip(stopwords, [0]*len(stopwords)))
        prem_stopwords = partial(remove_stopwords, stopwords=stopword_dict)
        tmp = column.str.split()
        tmp = tmp.map(prem_stopwords)
        tmp = tmp.str.join(' ')
        column = tmp
    return column

def preprocess_table(dataframe, overlap_attr, id_attr, stopwords=None):
    objsc = StringContainer()
    projdf = dataframe[[overlap_attr, id_attr]]
    projdf = projdf.dropna()
    projdf[overlap_attr] = process_col(projdf[overlap_attr], stopwords)
    for row in projdf.itertuples():
        val = str2bytes(row[1])
        uid = row[-1]
        objsc.push_back(uid, val)
    return objsc


def probe(objtc, objinvindex, threshold):
    objprobe = OverlapProber()
    objprobe.probe(objtc, objinvindex, (double) threshold)
    return objprobe

def compute_overlap(row, threshold):
    return len(set(x[0]).intersection(x[1])) >= threshold

def block_table_chunks(ldf, rdf, l_key, r_key, l_attr, r_attr, tokenizer, threshold,  stopwords, l_out, r_out, l_prefix, r_prefix):
    lstrings = preprocess_table(ldf, l_attr, l_key, stopwords)
    invindex = build_inv_index(lstrings)
    ltokens = tokenize_strings(lstrings, tokenizer)
    res = probe(ltokens, invindex, threshold)
    lcol, rcol = l_prefix + l_key, r_prefix + r_key
    res = pd.DataFrame(res.get_pairids(), columns=[lcol, rcol])
    res = add_attrs(res, ldf, rdf, lcol, rcol, l_key, r_key, l_out, r_out,
                    l_prefix, r_prefix)
    return res

def block_candset_chunks(candset, ldf, rdf, fk_ltable, fk_rtable, l_key, r_key, l_attr, r_attr, tokenizer, stopwords):
    #ldf = ldf.dropna()
    #rdf = rdf.dropna()
    tmp = pd.DataFrame()
    l_prefix, r_prefix = '__blk_a_','__blk_b_'
    cdf = add_attrs(candset, ldf, rdf, fk_ltable, fk_rtable, l_key, r_key, [l_attr], [r_attr], l_prefix, r_prefix)
    l_chk, r_chk = l_prefix + l_attr, r_prefix + r_attr

    x = process_col(cdf[l_chk], stopwords)
    x = x.map(str2bytes).map(tokenizer.tokenize)

    y = process_col(cdf[r_chk], stopwords)
    y = y.map(str2bytes).map(tokenizer.tokenize)

    overlap_fn = partial(compute_overlap, threshold=threshold)
    tmp['x'] = x
    tmp['y'] = y
    valid = tmp.map(overlap_fn)
    res = candset[valid]
    return res

def block_tables_sm(A, B, l_key, r_key, l_attr, r_attr, tokenizer, threshold, l_out=None, r_out=None, l_prefix='l_', r_prefix='r_', nlchunks=1, nrchunks=1):
    lsplitted = splitdf(A, nlchunks)
    rsplitted = splitdf(B, nrchunks)
    l_projcols = get_proj_cols(l_key, l_attr, l_out)
    r_projcols = get_proj_cols(r_key, r_attr, r_out)

    results = []
    for i in xrange(nlchunks):
        # here what we project must include lout
        # projcols = get_proj_cols(l_key, l_attr, l_out)
        ldf = projdf(lsplitted[i], l_projcols)
        for j in xrange(nrchunks):
            # projcols = get_proj_cols(r_key, r_attr, r_out)
            rdf = projdf(rsplitted[j], r_projcols)
            res = block_table_chunks(ldf, rdf, l_key, r_key, l_attr, r_attr, tokenizer, threshold, l_out, r_out,l_prefix, r_prefix)
            results.append(res)
    df = concatdf(results)
    df = addid(df)
    return df

def block_candset_sm(candset, A, B, fk_ltable, fk_rtable, l_key, r_key, l_attr, r_attr, tokenizer, threshold, nchunks=1):
    candsplitted = splitdf(candset, nchunks)
    results = []
    ldf = projdf(A, [l_key, l_attr])
    rdf = projdf(B, [r_key, r_attr])
    for i in xrange(nchunks):
        res = block_candset_chunks(candsplitted[i], ldf, rdf, fk_ltable, fk_rtable, l_key, r_key, l_attr, r_attr)
        results.append(res)
    df = concatdf(results)
    return df

def block_tables_dk(A, B, l_key, r_key, l_attr, r_attr, tokenizer, threshold, l_out=None, r_out=None, l_prefix='l_', r_prefix='r_', nlchunks=1, nrchunks=1, scheduler=threaded.get, compute=True):
    lsplitted = delayed(splitdf)(A, nlchunks)
    rsplitted = delayed(splitdf)(B, nrchunks)
    l_projcols = get_proj_cols(l_key, l_attr, l_out)
    r_projcols = get_proj_cols(r_key, r_attr, r_out)

    results = []
    for i in xrange(nlchunks):
        # here what we project must include lout
        # projcols = get_proj_cols(l_key, l_attr, l_out)
        ldf = delayed(projdf)(lsplitted[i], l_projcols)
        for j in xrange(nrchunks):
            # projcols = get_proj_cols(r_key, r_attr, r_out)
            rdf = delayed(projdf)(rsplitted[j], r_projcols)
            res = delayed(block_table_chunks)(ldf, rdf, l_key, r_key, l_attr, r_attr, tokenizer, threshold, l_out, r_out,l_prefix, r_prefix)
            results.append(res)
    df = delayed(concatdf)(results)
    df = delayed(addid)(df)

    if compute:
        return df.compute(get=scheduler)
    else:
        return df
def block_candset_dk(candset, A, B, fk_ltable, fk_rtable, l_key, r_key, l_attr, r_attr, tokenizer, threshold, nchunks=1, scheduler=threaded.get, compute=True):
    candsplitted = delayed(splitdf)(candset, nchunks)
    results = []
    ldf = delayed(projdf)(A, [l_key, l_attr])
    rdf = delayed(projdf)(B, [r_key, r_attr])
    for i in xrange(nchunks):
        res = delayed(block_candset_chunks)(candsplitted[i], ldf, rdf, fk_ltable, fk_rtable, l_key, r_key, l_attr, r_attr)
        results.append(res)
    df = delayed(concatdf)(results)
    if compute:
        return df.compute(get=scheduler)
    else:
        return df
