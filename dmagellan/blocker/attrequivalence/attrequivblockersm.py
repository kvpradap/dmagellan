# coding=utf-8
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