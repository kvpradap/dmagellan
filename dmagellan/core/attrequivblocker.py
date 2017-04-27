
import pandas as pd
from utils import splitdf, projdf, rename, add_attrs, concatdf, addid, get_proj_cols

from dask import delayed, threaded

def block_table_chunks(ldf, rdf, l_key, r_key, l_attr, r_attr, l_out, r_out, l_prefix, r_prefix):
    #ldf = ldf.dropna()
    #rdf = rdf.dropna()
    res = ldf.merge(rdf, left_on=l_attr, right_on=r_attr)
    lcol, rcol = l_key + '_x', r_key + '_y'
    res = projdf(res, [lcol, rcol])
    lcol, rcol = l_prefix + l_key, r_prefix + r_key
    res = rename(res, [lcol, rcol])
    res = add_attrs(res, ldf, rdf, lcol, rcol, l_key, r_key, l_out, r_out,
                    l_prefix, r_prefix)
    return res
def block_candset_chunks(candset, ldf, rdf, fk_ltable, fk_rtable, l_key, r_key, l_attr, r_attr):
    #ldf = ldf.dropna()
    #rdf = rdf.dropna()
    l_prefix, r_prefix = '__blk_a_','__blk_b_'
    cdf = add_attrs(candset, ldf, rdf, fk_ltable, fk_rtable, l_key, r_key, [l_attr], [r_attr], l_prefix, r_prefix)
    l_chk, r_chk = l_prefix + l_attr, r_prefix + r_attr
    res = candset[cdf[l_chk] == cdf[r_chk]]
    return res



def block_tables_sm(A, B, l_key, r_key, l_attr, r_attr, l_out=None, r_out=None, l_prefix='l_', r_prefix='r_', nlchunks=1, nrchunks=1):
    lsplitted = splitdf(A, nlchunks)
    rsplitted = splitdf(B, nrchunks)
    l_projcols = get_proj_cols(l_key, l_attr, l_out)
    r_projcols = get_proj_cols(r_key, r_attr, r_out)
    results = []
    for i in xrange(nlchunks):
        # here what we project must include lout

        ldf = projdf(lsplitted[i], l_projcols)
        for j in xrange(nrchunks):
            projcols = get_proj_cols(r_key, r_attr, r_out)
            rdf = projdf(rsplitted[j], r_projcols)
            res = block_table_chunks(ldf, rdf, l_key, r_key, l_attr, r_attr, l_out, r_out,l_prefix, r_prefix)
            results.append(res)
    df = concatdf(results)
    df = addid(df)
    return df

def block_candset_sm(candset, A, B, fk_ltable, fk_rtable, l_key, r_key, l_attr, r_attr, nchunks=1):
    candsplitted = splitdf(candset, nchunks)
    results = []
    ldf = projdf(A, [l_key, l_attr])
    rdf = projdf(B, [r_key, r_attr])
    for i in xrange(nchunks):
        res = block_candset_chunks(candsplitted[i], ldf, rdf, fk_ltable, fk_rtable, l_key, r_key, l_attr, r_attr)
        results.append(res)
    df = concatdf(results)
    return df




def block_tables_dk(A, B, l_key, r_key, l_attr, r_attr, l_out=None, r_out=None, l_prefix='l_', r_prefix='r_', nlchunks=1, nrchunks=1, scheduler=threaded.get, compute=True):
    lsplitted = delayed(splitdf)(A, nlchunks)
    rsplitted = delayed(splitdf)(B, nrchunks)
    l_projcols = get_proj_cols(l_key, l_attr, l_out)
    r_projcols = get_proj_cols(r_key, r_attr, r_out)

    results = []
    for i in xrange(nlchunks):
        # ldf = delayed(projdf)(lsplitted[i], [l_key, l_attr])
        ldf = delayed(projdf)(lsplitted[i], l_projcols)

        for j in xrange(nrchunks):
            rdf = delayed(projdf)(rsplitted[j], r_projcols)
            res = delayed(block_table_chunks)(ldf, rdf, l_key, r_key, l_attr, r_attr, l_out, r_out,
                                     l_prefix, r_prefix)
            results.append(res)
    df = delayed(concatdf)(results)
    df = delayed(addid)(df)
    if compute:
        return df.compute(get=scheduler)
    else:
        return df

def block_candset_dk(candset, A, B, fk_ltable, fk_rtable, l_key, r_key, l_attr, r_attr, nchunks=1, scheduler=threaded.get, compute=True):
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
