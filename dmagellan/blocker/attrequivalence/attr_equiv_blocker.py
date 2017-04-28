# coding=utf-8

from dask import delayed, threaded, cache
from dask.diagnostics import ProgressBar
from dask.cache import Cache

from dmagellan.blocker.attrequivalence.attrequivutils import get_attrs_to_project, \
    block_table_part, block_candset_part
from dmagellan.utils.py_utils.utils import splitdf, projdf, concatdf, addid


class AttrEquivalenceBlocker():

    def block_tables(self, ltable, rtable, l_key, r_key, l_block_attr, r_block_attr,
                     l_output_attrs=None, r_output_attrs=None, l_output_prefix='l_',
                     r_output_prefix='r_', nltable_chunks=1, allow_missing=False,
                     nrtable_chunks=1, scheduler=threaded.get, number_of_workers=None,
                     cache_size=1e9, compute=False, show_progress=True):
        # @todo validate inputs

        ltable_splitted = (splitdf)(ltable, nltable_chunks)
        rtable_splitted = (splitdf)(rtable, nrtable_chunks)

        l_proj_attrs = (get_attrs_to_project)(l_key, l_block_attr, l_output_attrs)
        r_proj_attrs = (get_attrs_to_project)(r_key, r_block_attr, r_output_attrs)

        # list ot accomodate results
        results = []
        for i in xrange(nltable_chunks):
            ltbl = (projdf)(ltable_splitted[i], l_proj_attrs)
            for j in xrange(nrtable_chunks):
                rtbl = (projdf)(rtable_splitted[j], r_proj_attrs)
                res = (block_table_part)(ltbl, rtbl, l_key, r_key, l_block_attr,
                                       r_block_attr, l_output_attrs, r_output_attrs,
                                                l_output_prefix, r_output_prefix)
                results.append(res)
        candset = (concatdf)(results)
        candset = (addid)(candset)

        if compute:
            cache = Cache(cache_size)
            if show_progress:
                with ProgressBar(), cache:
                    candset = candset.compute(get=scheduler)
            else:
                with cache:
                    candset = candset.compute(get=scheduler)


        return candset
