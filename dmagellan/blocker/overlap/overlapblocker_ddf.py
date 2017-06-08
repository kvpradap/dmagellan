# coding=utf-8
import string
from functools import partial

import pandas as pd
from dask import threaded, delayed

from dmagellan.blocker.blocker_utils import get_attrs_to_project, \
    get_lattrs_to_project, get_rattrs_to_project
from dmagellan.blocker.overlap.overlapprober import OverlapProber
from dmagellan.tokenizer.qgramtokenizer import QgramTokenizer
from dmagellan.tokenizer.whitespacetokenizer import WhiteSpaceTokenizer
from dmagellan.utils.cy_utils.stringcontainer import StringContainer
from dmagellan.utils.py_utils.utils import str2bytes, split_df, build_inv_index, \
    proj_df, tokenize_strings, add_attributes, concat_df, add_id, exec_dag, lsplit_df, \
    rsplit_df, candsplit_df, lproj_df, rproj_df, candproj_df

import pandas as pd

class OverlapBlocker(object):
    def __init__(self):
        self.stop_words = ['a', 'an', 'and', 'are', 'as', 'at',
                           'be', 'by', 'for', 'from',
                           'has', 'he', 'in', 'is', 'it',
                           'its', 'on', 'that', 'the', 'to', 'of',
                           'was', 'were', 'will', 'with']

    def _remove_stopwords(self, tokens, stopwords):
        out_tokens = []
        for token in tokens:
            if not stopwords.has_key(token):
                out_tokens.append(token)
        return out_tokens

    def _process_column(self, column, rem_stop_words):
        column = column.str.translate(None, string.punctuation)
        column = column.str.lower()
        if rem_stop_words and len(self.stop_words):
            dict_stopwords = dict(zip(self.stop_words, [0] * len(self.stop_words)))
            partial_rm_stopwords_fn = partial(self._remove_stopwords,
                                              stopwords=dict_stopwords)
            column = column.str.split().map(partial_rm_stopwords_fn).str.join(' ')
        return column

    def _preprocess_table(self, table, key_attr, block_attr, rem_stop_words):
        objsc = StringContainer()
        tbl = table[[block_attr, key_attr]]
        tbl.is_copy = False  # avoid setting with copy warning
        tbl[block_attr] = self._process_column(tbl[block_attr], rem_stop_words)
        for row in tbl.itertuples():
            val = str2bytes(row[1])
            key = row[-1]
            objsc.push_back(key, val)
        return objsc

    def _probe(self, objtc, objinvindex, threshold):
        objprobe = OverlapProber()
        objprobe.probe(objtc, objinvindex, float(threshold))
        return objprobe

    def _compute_overlap(self, row, threshold):
        return len(set(row[0]).intersection(row[1])) >= threshold

    def _block_table_part(self, ltable, rtable, l_key, r_key, l_block_attr,
                          r_block_attr,
                          tokenizer, threshold, rem_stop_words, l_output_attrs,
                          r_output_attrs, l_output_prefix, r_output_prefix):
        # ltable = ltable.compute()
        # rtable = rtable.compute()
        l_proj_attrs = (get_lattrs_to_project)(l_key, l_block_attr, l_output_attrs)
        r_proj_attrs = (get_rattrs_to_project)(r_key, r_block_attr, r_output_attrs)



        ltable = (lproj_df)(ltable, l_proj_attrs)
        rtable = (rproj_df)(rtable, r_proj_attrs)

        ltbl = ltable[~ltable[l_block_attr].isnull()]
        rtbl = rtable[~rtable[r_block_attr].isnull()]

        l_strings = self._preprocess_table(ltbl, l_key, l_block_attr, rem_stop_words)
        l_tokens = tokenize_strings(l_strings, tokenizer)
        inv_index = build_inv_index([l_tokens])

        r_strings = self._preprocess_table(rtbl, r_key, r_block_attr, rem_stop_words)
        r_tokens = tokenize_strings(r_strings, tokenizer)

        candset = self._probe(r_tokens, inv_index, threshold)
        fk_ltable, fk_rtable = l_output_prefix + l_key, r_output_prefix + r_key
        candset = pd.DataFrame(candset.get_pairids(), columns=[fk_ltable, fk_rtable])
        candset = add_attributes(candset, ltbl, rtbl, fk_ltable, fk_rtable, l_key,
                                 r_key, l_output_attrs, r_output_attrs,
                                 l_output_prefix, r_output_prefix)
        if not isinstance(candset, pd.DataFrame):
            print('Returning {0}'.format(candset))

        return candset

    def _block_candset_part(self, candset, ltable, rtable, fk_ltable, fk_rtable, l_key,
                            r_key, l_block_attr, r_block_attr, rem_stop_words, tokenizer,
                            threshold):
        if isinstance(candset, pd.DataFrame) and len(candset):
            l_proj_attrs = (get_lattrs_to_project)(l_key, l_block_attr)
            r_proj_attrs = (get_rattrs_to_project)(r_key, r_block_attr)

            # ltbl = (lproj_df)(ltable, l_proj_attrs)
            # rtbl = (rproj_df)(rtable, r_proj_attrs)

            ltable = (lproj_df)(ltable, l_proj_attrs)
            rtable = (rproj_df)(rtable, r_proj_attrs)

            ltbl = ltable[~ltable[l_block_attr].isnull()]  # this might be redundant
            rtbl = rtable[~rtable[r_block_attr].isnull()]  # this might be redundant
            l_prefix, r_prefix = '__blk_a_', '__blk_b_'

            temp_candset = add_attributes(candset, ltbl, rtbl, fk_ltable, fk_rtable, l_key,
                                          r_key, [l_block_attr],
                                          [r_block_attr], l_prefix, r_prefix)
            l_chk, r_chk = l_prefix + l_block_attr, r_prefix + r_block_attr

            x = self._process_column(temp_candset[l_chk], rem_stop_words)
            x = x.map(str2bytes).map(tokenizer.tokenize)

            y = self._process_column(temp_candset[r_chk], rem_stop_words)
            y = y.map(str2bytes).map(tokenizer.tokenize)

            overlap_fn = partial(self._compute_overlap, threshold=threshold)
            tmp = pd.DataFrame()
            tmp['x'] = x
            tmp['y'] = y
            # print(tmp)
            valid = tmp.apply(overlap_fn, raw=True, axis=1)
            # print(len(valid))
            valid_candset = candset[valid.values]
            return valid_candset
        else:
            candset

    def block_tables(self, ltable, rtable, l_key, r_key, l_block_attr,
                     r_block_attr, rem_stop_words=False, q_val=None, word_level=True,
                     overlap_size=1, l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='l_', r_output_prefix='r_',
                     nltable_chunks=1, nrtable_chunks=1, scheduler=threaded.get,
                     num_workers=None, cache_size=1e9, compute=False, show_progress=True):
        # @todo: validations.

        # ltable_splitted = (lsplit_df)(ltable, nltable_chunks)
        # rtable_splitted = (rsplit_df)(rtable, nrtable_chunks)

        # l_proj_attrs = (get_lattrs_to_project)(l_key, l_block_attr, l_output_attrs)
        # r_proj_attrs = (get_rattrs_to_project)(r_key, r_block_attr, r_output_attrs)

        if word_level == True:
            tokenizer = WhiteSpaceTokenizer()
        else:
            tokenizer = QgramTokenizer(q_val=q_val)

        results = []
        for i in xrange(nltable_chunks):
            # ltbl = (lproj_df)(ltable_splitted[i], l_proj_attrs)
            for j in xrange(nrtable_chunks):
                # rtbl = (rproj_df)(rtable_splitted[j], r_proj_attrs)
                result = delayed(self._block_table_part)(ltable.get_partition(i),
                                                         rtable.get_partition(j), l_key,
                                                         r_key,
                                                         l_block_attr,
                                                         r_block_attr, tokenizer,
                                                         overlap_size,
                                                         rem_stop_words, l_output_attrs,
                                                         r_output_attrs, l_output_prefix,
                                                         r_output_prefix)
                results.append(result)
        candset = delayed(concat_df)(results)
        candset = delayed(add_id)(candset)

        if compute:
            candset = exec_dag(candset, num_workers, cache_size, scheduler,
                               show_progress)

        return candset

    def block_candset(self, candset, ltable, rtable, fk_ltable, fk_rtable, l_key,
                      r_key, l_block_attr, r_block_attr, rem_stop_words=False, q_val=None,
                      word_level=True, overlap_size=1,
                      nchunks=1, scheduler=threaded.get,
                      num_workers=None, cache_size=1e9, compute=False,
                      show_progress=True):
        cand_splitted = delayed(candsplit_df)(candset, nchunks)

        # l_proj_attrs = (get_lattrs_to_project)(l_key, l_block_attr)
        # r_proj_attrs = (get_rattrs_to_project)(r_key, r_block_attr)
        #
        # ltbl = (lproj_df)(ltable, l_proj_attrs)
        # rtbl = (rproj_df)(rtable, r_proj_attrs)

        if word_level == True:
            tokenizer = WhiteSpaceTokenizer()
        else:
            tokenizer = QgramTokenizer(q_val=q_val)

        results = []
        for i in xrange(nchunks):
            result = delayed(self._block_candset_part)(cand_splitted[i], ltable, rtable,
                                                       fk_ltable,
                                                       fk_rtable, l_key, r_key,
                                                       l_block_attr,
                                                       r_block_attr, rem_stop_words,
                                                       tokenizer, overlap_size)
            results.append(result)

        valid_candset = delayed(concat_df)(results)
        if compute:
            valid_candset = exec_dag(valid_candset, num_workers, cache_size, scheduler,
                                     show_progress)
        return valid_candset


