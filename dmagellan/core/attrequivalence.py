
import pandas as pd
from utils import splitdf, projdf, rename, add_attrs, concatdf, addid

from dask import delayed, threaded

def block_table_chunks(ldf, rdf, l_key, r_key, l_attr, r_attr, l_out, r_out, l_prefix,
                        r_prefix):
    ldf = ldf.dropna()
    rdf = rdf.dropna()
    res = ldf.merge(rdf, left_on=l_attr, right_on=r_attr)
    lcol, rcol = l_key + '_x', r_key + '_y'
    res = projdf(res, [lcol, rcol])
    lcol, rcol = l_prefix + l_key, r_prefix + r_key
    res = rename(res, [lcol, rcol])
    res = add_attrs(res, ldf, rdf, lcol, rcol, l_key, r_key, l_out, r_out,
                    l_prefix, r_prefix)
    return res


def block_tables_sm(A, B, l_key, r_key, l_attr, r_attr, l_out=None, r_out=None, l_prefix='l_',
                        r_prefix='r_', nlchunks=1, nrchunks=1):
    lsplitted = splitdf(A, nlchunks)
    rsplitted = splitdf(B, nrchunks)

    results = []
    for i in xrange(nlchunks):
        ldf = projdf(lsplitted[i], [l_key, l_attr])
        for j in xrange(nrchunks):
            rdf = projdf(rsplitted[j], [r_key, r_attr])
            res = block_table_chunks(ldf, rdf, l_key, r_key, l_attr, r_attr, l_out, r_out,
                                     l_prefix, r_prefix)
            results.append(res)
    df = concatdf(results)
    df = addid(df)
    return df



def block_tables_dk(A, B, l_key, r_key, l_attr, r_attr, l_out=None, r_out=None, l_prefix='l_',
                        r_prefix='r_', nlchunks=1, nrchunks=1, scheduler=threaded.get,
                    compute=True):
    lsplitted = delayed(splitdf)(A, nlchunks)
    rsplitted = delayed(splitdf)(B, nrchunks)

    results = []
    for i in xrange(nlchunks):
        ldf = delayed(projdf)(lsplitted[i], [l_key, l_attr])
        for j in xrange(nrchunks):
            rdf = delayed(projdf)(rsplitted[j], [r_key, r_attr])
            res = delayed(block_table_chunks)(ldf, rdf, l_key, r_key, l_attr, r_attr, l_out, r_out,
                                     l_prefix, r_prefix)
            results.append(res)
    df = delayed(concatdf)(results)
    df = delayed(addid)(df)
    if compute:
        return df.compute(get=scheduler)
    else:
        return df







