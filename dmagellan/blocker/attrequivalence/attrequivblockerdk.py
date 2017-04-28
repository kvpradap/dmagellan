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
