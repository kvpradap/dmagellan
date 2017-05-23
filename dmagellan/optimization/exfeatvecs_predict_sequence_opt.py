from .dictutils import *
from dask.optimize import cull, fuse


def delay_concat(dag, start='_extract_feature_vecs_part', search='concat_df', copy=False):
    if isinstance(dag, dict):
        node_list = get_blocker_subgraphs_ordered(dag, start, search)
    else:
        node_list = dag
    assert(len(node_list) == 2)
    node_list = remove_concat_split_for_exfeatpredict_list(node_list)
    out_dag = dict(convert_ldicts_to_sdict(node_list))
    return out_dag

def fuse_dag(dag, copy=False):
    if copy:
        dag = deepcopy(dag)
    dag = dict(dag)
    dsk, dep = cull(dag, dag.keys())
    dsk, dep = fuse(dsk, ave_width=1, rename_keys=False)
    return dsk



