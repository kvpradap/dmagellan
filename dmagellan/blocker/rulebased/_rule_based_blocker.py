import py_stringsimjoin as ssj
import six


class RuleBasedBlocker:
    def __init__(self, *args, **kwargs):
        feature_table = kwargs.pop('feature_table', None)
        self.feature_table = feature_table
        self.rules = OrderedDict()

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

    def delete_rule(self, rule_name):

        del self.rules[rule_name]
        del self.rule_source[rule_name]
        del self.rule_str[rule_name]
        del self.rule_ft[rule_name]

        return True

    def view_rule(self, rule_name):
        assert rule_name in self.rules.keys(), 'Rule name not in current set of rules'
        print(self.rule_source[rule_name])

    def get_rule_names(self):
        return self.rules.keys()

    def get_rule(self, rule_name):
        assert rule_name in self.rules.keys(), 'Rule name not in current set of rules'
        return self.rules[rule_name]

    def set_feature_table(self, feature_table):
        self.feature_table = feature_table

    def apply_rules(self, ltuple, rtuple):
        for fn in self.rules.values():
            # here if the function returns true, then the tuple pair must be dropped.
            res = fn(ltuple, rtuple)
            if res == True:
                return res
        return False

    def apply_rules_excluding_rule(self, ltuple, rtuple, rule_to_exclude):
        for rule_name in self.rules.keys():
            if rule_name != rule_to_exclude:
                fn = self.rules[rule_name]
                # here if fn returns true, then the tuple pair must be dropped.
                res = fn(ltuple, rtuple)
                if res == True:
                    return res
        return False

    def parse_conjunct(self, conjunct, rule_name):
        # TODO: Make parsing more robust using pyparsing
        feature_table = self.rule_ft[rule_name]
        vals = conjunct.split('(')
        feature_name = vals[0].strip()
        if feature_name not in feature_table.feature_name.values:
            logger.error('Feature ' + feature_name + ' is not present in ' +
                         'supplied feature table. Cannot apply rules.')
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

    def block_tables_with_filters(self, l_df, r_df, l_key, r_key):
        for rule_name in self.rules.keys():
            # first check if a rule is filterable
            if self.is_rule_filterable(rule_name):
                candset = self.apply_filterable_rule(rule_name, l_df, r_df, l_key, r_key)
                if candset is not None:
                    # rule was filterable
                    return candset, rule_name
        return None, None

    def is_rule_filterable(self, rule_name):
        # a rule is filterable if all the conjuncts in the conjunct list
        # are filterable
        conjunct_list = self.rule_str[rule_name]
        for conjunct in conjunct_list:
            res = self.is_conjunct_filterable(conjunct, rule_name)
            if res != True:
                return res
        return True

    def is_conjunct_filterable(self, conjunct, rule_name):
        # a conjunct is filterable if it uses
        # a filterable sim function (jaccard, cosine, dice, ...),
        # an allowed operator (<, <=),
        is_auto_gen, sim_fn, l_attr, r_attr, l_tok, r_tok, op, th = self.parse_conjunct(
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

    def apply_filterable_rule(self, rule_name, l_df, r_df, l_key, r_key,
                              l_output_prefix, r_output_prefix):
        candset = None
        conjunct_list = self.rule_str[rule_name]
        for conjunct in conjunct_list:
            is_auto_gen, sim_fn, l_attr, r_attr, l_tok, r_tok, op, th = self.parse_conjunct(
                conjunct, rule_name)

            if l_tok == 'dlm_dc0':
                tokenizer = WhitespaceTokenizer(return_set=True)
            elif l_tok == 'qgm_3':
                tokenizer = QgramTokenizer(q_val=3, return_set=True)

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

            ssj.dataframe_column_to_str(l_df, l_attr, inplace=True)
            ssj.dataframe_column_to_str(r_df, r_attr, inplace=True)

            if join_fn == ssj.edit_distance_join:
                tokenizer = QgramTokenizer(q_val=2, return_set=False)
                c_df = join_fn(l_df, r_df, l_key, r_key, l_attr, r_attr,
                               float(th), comp_op, allow_missing=False,
                               # need to revisit allow_missing
                               out_sim_score=False,
                               l_out_prefix=l_output_prefix,
                               r_out_prefix=r_output_prefix,
                               show_progress=False, tokenizer=tokenizer)
            else:
                c_df = join_fn(l_df, r_df, l_key, r_key, l_attr, r_attr,
                               tokenizer, float(th), comp_op, allow_empty=True,
                               allow_missing=False,
                               l_out_prefix=l_output_prefix,
                               r_out_prefix=r_output_prefix,
                               out_sim_score=False)
            if candset is not None:
                # union the candset of this conjunct with the existing candset
                candset = pd.concat([candset, c_df]).drop_duplicates(
                    [l_output_prefix + l_key,
                     r_output_prefix + r_key]).reset_index(drop=True)
            else:
                # candset from the first conjunct of the rule
                candset = c_df
            return candset

    def block_candset_excluding_rule(self, c_df, l_df, r_df, l_key, r_key,
                                     fk_ltable, fk_rtable, rule_to_exclude,
                                     show_progress, n_jobs):

        # # determine number of processes to launch parallely
        n_procs = self.get_num_procs(n_jobs, len(c_df))

        # # list to keep track of valid i

        valid = _block_candset_excluding_rule_split(c_df, l_df, r_df,
                                                    l_key, r_key,
                                                    fk_ltable, fk_rtable,
                                                    rule_to_exclude)
       # construct output candset
        if len(c_df) > 0:
            candset = c_df[valid]
        else:
            candset = pd.DataFrame(columns=c_df.columns)

        # return candidate set
        return candset

    def block_tables_without_filters(self, ltable, rtable, l_key, r_key,
                                     l_output_prefix, r_output_prefix):
        l_key_vals = ltable[l_key].values
        r_key_vals = rtable[r_key].values

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

        fk_ltable, fk_rtable = l_output_prefix + l_key, r_output_prefix + r_key
        candset = pd.DataFrame(valid_pairs, columns=[fk_ltable, fk_rtable])
        return candset

