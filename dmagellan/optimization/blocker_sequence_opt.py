# coding=utf-8

# three opts
# 1. rearrange the blockers
# 2. delay concatenating the results
# 3. fuse the nodes

# imports
from .dictutils import *
from dask.optimize import cull, fuse

# opt 1
def rearrange_blockers(dag):
    blocker_list = get_blocker_subgraphs(dag)
    ordered_dag = order_candset_blockers(blocker_list)
    ordered_dag = upd_blocker_inp_links(ordered_dag)
    ordered_dag = dict(convert_ldicts_to_sdict(ordered_dag))
    return ordered_dag

# opt1-1
def move_addid(dag, start='_block_table_part', search='concat_df', copy=False):
    if isinstance(dag, dict):
        blocker_list = get_blocker_subgraphs_ordered(dag, start, search)
    else:
        blocker_list = dag
    blocker_list = move_add_id_to_last(blocker_list, copy=copy)
    out_dag = dict(convert_ldicts_to_sdict(blocker_list))
    return out_dag



# opt 2
def delay_concat(dag, start='_block_table_part', search='concat_df', copy=False):
    if copy:
        dag = deepcopy(dag)
    if isinstance(dag, dict):
        blocker_list = get_blocker_subgraphs_ordered(dag, start, search)
    else:
        blocker_list = dag
    blocker_list = remove_concat_split_for_blocker_list(blocker_list)
    out_dag = dict(convert_ldicts_to_sdict(blocker_list))
    return out_dag

# update the input tables
def update_input_tables(dag, copy=False):
    for head in get_keys(dag, '_block_table_part'):
        h_ltbl, h_rtbl = dag[head][1], dag[head][2]
        dag = set_ltable_rtable_for_cand_deps(dag, head, h_ltbl, h_rtbl)
    return dag

def fuse_dag(dag, copy=False):
#     d =dict(convert_ldicts_to_sdict(c_blocker_list))
    if copy:
        dag = deepcopy(dag)
    dag = dict(dag)
    dsk, dep = cull(dag, dag.keys())
    dsk, dep = fuse(dsk, ave_width=1, rename_keys=False)
    return dsk

