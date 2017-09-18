# coding=utf-8

from collections import OrderedDict

import pandas as pd
import py_stringsimjoin as ssj
import six
from dask import threaded, delayed
from py_stringmatching.tokenizer.qgram_tokenizer import QgramTokenizer
from py_stringmatching.tokenizer.whitespace_tokenizer import WhitespaceTokenizer

from dmagellan.blocker.blocker_utils import get_attrs_to_project, \
    get_lattrs_to_project, get_rattrs_to_project
from dmagellan.utils.py_utils.utils import split_df, proj_df, add_attributes, concat_df, \
    add_id, exec_dag, lsplit_df, rsplit_df, candsplit_df, lproj_df, rproj_df, candproj_df


class RuleBasedBlocker(object):
    def __init__(self, *args, **kwargs):
        feature_table = kwargs.pop('feature_table', None)
        self.feature_table = feature_table
        self.rules = OrderedDict()
        self.ltable_attrs = kwargs.pop('ltable_attrs', None)
        self.rtable_attrs = kwargs.pop('rtable_attrs', None)

        self.rule_str = OrderedDict()
        self.rule_ft = OrderedDict()
        self.filterable_sim_fns = {'jaccard', 'cosine', 'dice', 'overlap_coeff'}
        self.allowed_ops = {'<', '<='}

        self.rule_source = OrderedDict()
        self.rule_cnt = 0

    def _create_rule(self, conjunct_list, feature_table, rule_name):
        if rule_name is None:
            # set the rule name automatically
            name = '_rule_' + str(self.rule_cnt)
            self.rule_cnt += 1
        else:
            # use the rule name supplied by the user
            name = rule_name

        # create function string
        fn_str = 'def ' + name + '(ltuple, rtuple):\n'

        # add 4 tabs
        fn_str += '    '
        fn_str += 'return ' + ' and '.join(conjunct_list)

        if feature_table is not None:
            feat_dict = dict(
                zip(feature_table['feature_name'], feature_table['function']))
        else:
            feat_dict = dict(zip(self.feature_table['feature_name'],
                                 self.feature_table['function']))

        six.exec_(fn_str, feat_dict)

        return feat_dict[name], name, fn_str

    def add_rule(self, conjunct_list, feature_table=None, rule_name=None):
        if not isinstance(conjunct_list, list):
            conjunct_list = [conjunct_list]

        fn, name, fn_str = self._create_rule(conjunct_list, feature_table, rule_name)

        self.rules[name] = fn
        self.rule_source[name] = fn_str
        self.rule_str[name] = conjunct_list
        if feature_table is not None:
            self.rule_ft[name] = feature_table
        else:
            self.rule_ft[name] = self.feature_table

        return name

    def set_feature_table(self, feature_table):
        self.feature_table = feature_table

    def set_table_attrs(self, ltable_attrs, rtable_attrs):
        self.ltable_attrs = ltable_attrs
        self.rtable_attrs = rtable_attrs

    def block_tables(self, ltable, rtable, l_key, r_key, l_output_attrs=None,
                     r_output_attrs=None, l_output_prefix='l_', r_output_prefix='r_',
                     nltable_chunks=1, nrtable_chunks=1, scheduler=threaded.get,
                     num_workers=None, cache_size=1e9, compute=False, show_progress=True):
        ltable_splitted = (lsplit_df)(ltable, nltable_chunks)
        rtable_splitted = (rsplit_df)(rtable, nrtable_chunks)

        # l_proj_attrs = (get_lattrs_to_project)(l_key, self.ltable_attrs,
        #                                       l_output_attrs)
        # # needs to be modified as self.ltable_attrs can be None.
        # r_proj_attrs = (get_rattrs_to_project)(r_key, self.rtable_attrs,
        #                                       r_output_attrs)
        results = []
        for i in xrange(nltable_chunks):
            # ltbl = (lproj_df)(ltable_splitted[i], l_proj_attrs)
            for j in xrange(nrtable_chunks):
                # rtbl = (rproj_df)(rtable_splitted[j], r_proj_attrs)
                result = delayed(self._block_tables_part)(ltable_splitted[i],
                                                          rtable_splitted[j],
                                                          l_key, r_key,
                                                   l_output_attrs, r_output_attrs,
                                                   l_output_prefix, r_output_prefix)
                results.append(result)
        candset = delayed(concat_df)(results)
        candset = delayed(add_id)(candset)
        if compute:
            candset = exec_dag(candset, num_workers, cache_size, scheduler,
                               show_progress)
        return candset

    def block_candset(self, candset, ltable, rtable, fk_ltable, fk_rtable, l_key, r_key,
                      nchunks=1, scheduler=threaded.get, num_workers=None,
                      cache_size=1e9, compute=False, show_progress=True):
        candset_splitted = delayed(candsplit_df)(candset, nchunks)
        # l_proj_attrs = (get_lattrs_to_project)(l_key, self.ltable_attrs)
        # r_proj_attrs = (get_rattrs_to_project)(r_key, self.rtable_attrs)
        #
        # ltbl = (lproj_df)(ltable, l_proj_attrs)
        # rtbl = (rproj_df)(rtable, r_proj_attrs)

        results = []
        for i in xrange(nchunks):
            result = delayed(self._block_candset_part)(candset_splitted[i], ltable,
                                                rtable,
                                                fk_ltable, fk_rtable, l_key, r_key)
            results.append(result)
        valid_candset = delayed(concat_df)(results)

        if compute:
            valid_candset = exec_dag(valid_candset, num_workers, cache_size,
                                     scheduler,
                                     show_progress)
        return valid_candset

    def _block_tables_part(self, ltable, rtable, l_key, r_key, l_output_attrs,
                           r_output_attrs, l_output_prefix, r_output_prefix):
        fk_ltable, fk_rtable = l_output_prefix + l_key, r_output_prefix + r_key
        candset, rule_applied = self._block_tables_with_filters(ltable, rtable, l_key,
                                                                r_key)

        l_proj_attrs = (get_lattrs_to_project)(l_key, self.ltable_attrs,
                                              l_output_attrs)
        # needs to be modified as self.ltable_attrs can be None.
        r_proj_attrs = (get_rattrs_to_project)(r_key, self.rtable_attrs,
                                              r_output_attrs)
        ltable = lproj_df(ltable, l_proj_attrs)
        rtable = rproj_df(rtable, r_proj_attrs)
        if candset is None:
            candset = self._block_tables_without_filters(ltable, rtable, l_key, r_key,
                                                         fk_ltable, fk_rtable)
        elif len(self.rules) > 1:

            candset = self._block_candset_excluding_rule(candset, ltable, rtable,
                                                         fk_ltable, fk_rtable,
                                                         l_key, r_key,
                                                         rule_applied)
        # candset = pd.DataFrame(candset, )candset
        candset = add_attributes(candset, ltable, rtable, fk_ltable, fk_rtable, l_key,
                                 r_key, l_output_attrs, r_output_attrs,
                                 l_output_prefix, r_output_prefix)

        return candset

    def _block_candset_part(self, candset, ltable, rtable, fk_ltable, fk_rtable, l_key,
                            r_key):
        if isinstance(candset, pd.DataFrame) and len(candset):
            l_proj_attrs = (get_lattrs_to_project)(l_key, self.ltable_attrs)
            r_proj_attrs = (get_rattrs_to_project)(r_key, self.rtable_attrs)

            ltbl = (lproj_df)(ltable, l_proj_attrs)
            rtbl = (rproj_df)(rtable, r_proj_attrs)

            candset = self._block_candset_excluding_rule(candset, ltbl, rtbl,
                                                         fk_ltable, fk_rtable,
                                                         l_key, r_key)
            if not isinstance(candset, pd.DataFrame):
                print('Returning {0}'.format(candset))

        return candset

    def _block_tables_with_filters(self, ltable, rtable, l_key, r_key):
        for rule_name in self.rules.keys():
            if self._is_rule_filterable(rule_name):
                candset = self._apply_filterable_rule(rule_name, ltable, rtable, l_key,
                                                      r_key)
                if candset is not None:
                    return candset, rule_name
        return None, None

    def _is_rule_filterable(self, rule_name):
        # a rule is filterable if all the conjuncts in the conjunct list
        # are filterable
        conjunct_list = self.rule_str[rule_name]
        for conjunct in conjunct_list:
            res = self._is_conjunct_filterable(conjunct, rule_name)
            if res != True:
                return res
        return True

    def _is_conjunct_filterable(self, conjunct, rule_name):
        # a conjunct is filterable if it uses
        # a filterable sim function (jaccard, cosine, dice, ...),
        # an allowed operator (<, <=),
        is_auto_gen, sim_fn, l_attr, r_attr, l_tok, r_tok, op, th = self._parse_conjunct(
            conjunct, rule_name)
        if is_auto_gen != True:
            # conjunct not filterable as the feature is not auto generated
            return False
        if sim_fn == 'lev_dist':
            if op == '>' or op == '>=':
                return True
            else:
                # conjunct not filterable due to unsupported operator
                return False
        if l_tok != r_tok:
            # conjunct not filterable because left and right tokenizers mismatch
            return False
        if sim_fn not in self.filterable_sim_fns:
            # conjunct not filterable due to unsupported sim_fn
            return False
        if op not in self.allowed_ops:
            # conjunct not filterable due to unsupported operator
            return False
        # conjunct is filterable
        return True

    def _apply_filterable_rule(self, rule_name, ltable, rtable, l_key, r_key):
        candset = None
        conjunct_list = self.rule_str[rule_name]
        for conjunct in conjunct_list:
            is_auto_gen, sim_fn, l_attr, r_attr, l_tok, r_tok, op, \
            th = self._parse_conjunct(
                conjunct, rule_name)

            if l_tok == 'dlm_dc0':
                tokenizer = WhitespaceTokenizer(return_set=True)
            elif l_tok == 'qgm_3':
                tokenizer = QgramTokenizer(qval=3, return_set=True)

            if sim_fn == 'jaccard':
                join_fn = ssj.jaccard_join
            elif sim_fn == 'cosine':
                join_fn = ssj.cosine_join
            elif sim_fn == 'dice':
                join_fn = ssj.dice_join
            elif sim_fn == 'overlap_coeff':
                join_fn = ssj.overlap_coefficient_join
            elif sim_fn == 'lev_dist':
                join_fn = ssj.edit_distance_join

            if join_fn == ssj.edit_distance_join:
                comp_op = '<='
                if op == '>=':
                    comp_op = '<'
            else:
                comp_op = '>='
                if op == '<=':
                    comp_op = '>'

            ssj.dataframe_column_to_str(ltable, l_attr, inplace=True)
            ssj.dataframe_column_to_str(rtable, r_attr, inplace=True)

            if join_fn == ssj.edit_distance_join:
                tokenizer = QgramTokenizer(qval=2, return_set=False)
                c_df = join_fn(ltable, rtable, l_key, r_key, l_attr, r_attr,
                               float(th), comp_op, allow_missing=True,
                               # need to revisit allow_missing
                               out_sim_score=False,
                               l_out_prefix='l_',
                               r_out_prefix='r_',
                               show_progress=False, tokenizer=tokenizer)
            else:
                c_df = join_fn(ltable, rtable, l_key, r_key, l_attr, r_attr,
                               tokenizer, float(th), comp_op, allow_empty=True,
                               allow_missing=True,
                               l_out_prefix='l_',
                               r_out_prefix='r_',
                               out_sim_score=False)
                #c_df.drop('_id', axis=1)
            if candset is not None:
                # union the candset of this conjunct with the existing candset
                candset = pd.concat([candset, c_df]).drop_duplicates(
                    [l_output_prefix + l_key,
                     r_output_prefix + r_key]).reset_index(drop=True)
            else:
                # candset from the first conjunct of the rule
                candset = c_df
        return candset

    def _parse_conjunct(self, conjunct, rule_name):
        # TODO: Make parsing more robust using pyparsing
        feature_table = self.rule_ft[rule_name]
        vals = conjunct.split('(')
        feature_name = vals[0].strip()
        if feature_name not in feature_table.feature_name.values:
            raise AssertionError(
                'Feature ' + feature_name + ' is not present ' +
                'in supplied feature table. Cannot apply rules.')
        vals1 = vals[1].split(')')
        vals2 = vals1[1].strip()
        vals3 = vals2.split()
        operator = vals3[0].strip()
        threshold = vals3[1].strip()
        ft_df = feature_table.set_index('feature_name')
        return (ft_df.ix[feature_name]['is_auto_generated'],
                ft_df.ix[feature_name]['simfunction'],
                ft_df.ix[feature_name]['left_attribute'],
                ft_df.ix[feature_name]['right_attribute'],
                ft_df.ix[feature_name]['left_attr_tokenizer'],
                ft_df.ix[feature_name]['right_attr_tokenizer'],
                operator, threshold)

    def _block_tables_without_filters(self, ltable, rtable, l_key, r_key, fk_ltable,
                                      fk_rtable):
        l_key_vals = ltable[l_key].values
        r_key_vals = rtable[r_key].values
        if self.ltable_attrs != None:
            ltbl = ltable[self.ltable_attrs]
        else:
            ltbl = ltable

        if self.rtable_attrs != None:
            rtbl = rtable[self.rtable_attrs]
        else:
            rtbl = rtable

        l_dict = {}
        r_dict = {}

        for i in xrange(len(ltbl)):
            l_dict[l_key_vals[i]] = ltbl.iloc[i]

        for i in xrange(len(rtbl)):
            r_dict[r_key_vals[i]] = rtbl.iloc[i]

        valid_pairs = []
        for l_id in l_dict:
            l_tuple = l_dict[l_id]
            for r_id in r_dict:
                r_tuple = r_dict[r_id]
                res = self.apply_rules(l_tuple, r_tuple)
                if not res:
                    valid_pairs.append([l_id, r_id])

        candset = pd.DataFrame(valid_pairs, columns=[fk_ltable, fk_rtable])
        return candset

    def apply_rules(self, ltuple, rtuple):
        for fn in self.rules.values():
            # here if the function returns true, then the tuple pair must be dropped.
            res = fn(ltuple, rtuple)
            if res == True:
                return res
        return False

    def _block_candset_excluding_rule(self, candset, ltable, rtable, fk_ltable,
                                      fk_rtable, l_key, r_key,
                                      rule_to_exclude=None):
        ltbl = ltable.set_index(l_key, drop=False)
        rtbl = rtable.set_index(r_key, drop=False)
        if self.ltable_attrs != None:
            ltbl = ltbl[self.ltable_attrs]

        if self.rtable_attrs != None:
            rtbl = rtbl[self.rtable_attrs]

        c_df = candset[[fk_ltable, fk_rtable]]
        l_dict = {}
        r_dict = {}
        # list to keep track of valid ids
        valid = []
        l_id_pos = list(c_df.columns).index(fk_ltable)
        r_id_pos = list(c_df.columns).index(fk_rtable)
        for row in c_df.itertuples(index=False):
            row_lkey = row[l_id_pos]
            if row_lkey not in l_dict:
                l_tuple = ltbl.ix[row_lkey]
                l_dict[row_lkey] = l_tuple
            else:
                l_tuple = l_dict[row_lkey]

            # # get rtuple, try dictionary first, then dataframe
            row_rkey = row[r_id_pos]
            if row_rkey not in r_dict:
                r_tuple = rtbl.ix[row_rkey]
                r_dict[row_rkey] = r_tuple
            else:
                r_tuple = r_dict[row_rkey]
            res = self.apply_rules_excluding_rule(l_tuple, r_tuple, rule_to_exclude)
            if not res:
                valid.append(True)
            else:
                valid.append(False)
        return candset[valid]

    def apply_rules_excluding_rule(self, ltuple, rtuple, rule_to_exclude):
        for rule_name in self.rules.keys():
            if rule_name != rule_to_exclude:
                fn = self.rules[rule_name]
                # here if fn returns true, then the tuple pair must be dropped.
                res = fn(ltuple, rtuple)
                if res == True:
                    return res
        return False
