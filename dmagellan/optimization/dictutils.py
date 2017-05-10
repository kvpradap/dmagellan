import dask
import inspect
from dask.optimize import  key_split
from dask.sharedict import ShareDict
from dask.dot import dot_graph
from copy import deepcopy
from dask.delayed import Delayed




def get_class_that_defined_method(meth):
    for cls in inspect.getmro(meth.im_class):
        if meth.__name__ in cls.__dict__:
            return cls
    return None


def get_keys(d, key):
    keys = d.keys()
    l = []
    for k in keys:
        v = key_split(k)
        if v == key:
            l.append(k)
    return l


def get_dependencies(dag, node):
    if not dag.has_key(node):
        raise AssertionError('Key {0} not in dag'.format(key))
    val = dag[node]
    input_keys = []
    for v in val[1:]:
        if type(v) == type(''):
            if dag.has_key(v):
                input_keys.append(v)
        elif type(v) == type([]):
            for k in v:
                if type(k) == type(''):
                    if dag.has_key(k):
                        input_keys.append(k)
    return input_keys


def get_dependents(dag, node):
    keys = dag.keys()
    dependents = []
    for k in keys:
        dependencies = get_dependencies(dag, k)
        if node in dependencies:
            dependents.append(k)

    return dependents


def get_subgraph_as_dict_for_node(dag, node):
    d = {}
    bfs_list = []
    bfs_list.append(node)
    while len(bfs_list):
        n = bfs_list.pop(0)
        d[n] = dag[n]
        if key_split(n) != 'lsplit_df' and key_split(n) != 'rsplit_df' and key_split(n)\
                != 'candsplit_df':
            for dep in get_dependencies(dag, n):
                bfs_list.append(dep)

    return d


def ishead(dag, concat_node):
    dependents = get_dependents(dag, concat_node)
    if len(dependents):
        if key_split(dependents[0]) == 'add_id':
            return True
    return False


def get_blocker_subgraphs(dag):
    concat_nodes = get_keys(dag, 'concat_df')
    subgraphs = []
    for concat_node in concat_nodes:
        if ishead(dag, concat_node):
            addid_node = get_dependents(dag, concat_node)[0]
            d = get_subgraph_as_dict_for_node(dag, addid_node)
        else:
            d = get_subgraph_as_dict_for_node(dag, concat_node)
        subgraphs.append(d)
    return subgraphs


def get_table_chunks(blocker):
    b = get_keys(blocker, '_block_table_part')
    # r_split = get_keys(blocker, 'rsplit_df')[0]

    # assert (len(split_dfs) == 2)
    # chunks = {}
    # chunks['nlchunks'] = l_split[2]
    # chunks['nrchunks'] = r_split[2]
    return b


def get_candset_chunks(blocker):
    split_dfs = get_keys(blocker, 'candsplit_df')
    assert (len(split_dfs) == 1)
    return blocker[split_dfs[0]][2]


def get_blocker_type(blocker, is_cand):
    if is_cand:
        t = str(get_class_that_defined_method(
            blocker[get_keys(blocker, '_block_candset_part')[0]][0])).split('.')[-1]
        return t
    else:
        t = str(get_class_that_defined_method(
            blocker[get_keys(blocker, '_block_table_part')[0]][0])).split('.')[-1]
        return t

def iscand(blocker):
    return len(get_keys(blocker, '_block_candset_part')) != 0

def get_blocker_props(blocker):
    d = {}
    d['iscand'] = iscand(blocker)
    d['type'] = get_blocker_type(blocker, d['iscand'])
    return d

def print_blocker_list_props(blocker_list):
    for blocker in blocker_list:
        print(get_blocker_props(blocker))

def order_candset_blockers(blockers):
    attr_blockers = []
    overlap_blockers = []
    rulebased_blockers = []
    bb_blockers = []
    blocker_list = []
    for blocker in blockers:
        props = get_blocker_props(blocker)
        if not props['iscand']:
            blocker_list.append(blocker)
        elif props['type'] == 'AttrEquivalenceBlocker':
            attr_blockers.append(blocker)
        elif props['type'] == 'OverlapBlocker':
            overlap_blockers.append(blocker)
        elif props['type'] == 'BlackBoxBlocker':
            bb_blockers.append(blocker)
        else:
            rulebased_blockers.append(blocker)

    # add head
    for blocker in attr_blockers:
        blocker_list.append(blocker)
    for blocker in overlap_blockers:
        blocker_list.append(blocker)
    for blocker in rulebased_blockers:
        blocker_list.append(blocker)
    for blocker in bb_blockers:
        blocker_list.append(blocker)
    return blocker_list


def upd_split_df_input_node(blocker, new):
    split_df_key = get_keys(blocker, 'candsplit_df')[0]
    node = blocker[split_df_key]
    node = list(node)
    node[1] = new
    blocker[split_df_key] = tuple(node)
    return blocker


def convert_ldicts_to_sdict(blocker_list):
    s = ShareDict()
    for blocker in blocker_list:
        for k, v in blocker.iteritems():
            d = {}
            d[k] = v
            s.update_with_key(d, key=k)
    return s


def upd_blocker_inp_links(blocker_list):
    for i in range(len(blocker_list) - 1):
        cur = blocker_list[i]
        nxt = blocker_list[i + 1]

        if i == 0:
            # case: head
            assert (iscand(cur) == False)
            upd_split_df_input_node(nxt, get_keys(cur, 'add_id')[0])
        else:
            # case: candset
            assert (iscand(cur) == True)
            upd_split_df_input_node(nxt, get_keys(cur, 'concat_df')[0])
    return blocker_list





def move_add_id_to_last(blocker_list, copy=True):
    if copy:
        blocker_list = deepcopy(blocker_list)

    assert (len(blocker_list) >= 2)
    head = blocker_list[0]
    head_nxt = blocker_list[1]
    tail = blocker_list[-1]
    assert (iscand(head) == False)
    assert (iscand(tail) == True)

    # moving the add id to last
    head_add_id_key = get_keys(head, 'add_id')[0]
    tail_concat_key = get_keys(tail, 'concat_df')[0]

    head_add_id_values = list(head[head_add_id_key])
    head_add_id_values[1] = tail_concat_key

    # adding to tail
    tail[head_add_id_key] = tuple(head_add_id_values)

    # removing from head
    del head[head_add_id_key]

    # need to set the input of concat_df from head to to the
    # input of split_df in the head_next node
    head_concat_node_key = get_keys(head, 'concat_df')[0]
    head_nxt_split_node_key = get_keys(head_nxt, 'candsplit_df')[0]

    head_nxt_split_node_vals = list(head_nxt[head_nxt_split_node_key])
    head_nxt_split_node_vals[1] = head_concat_node_key
    head_nxt[head_nxt_split_node_key] = tuple(head_nxt_split_node_vals)

    return blocker_list


def vis(blocker_list):
    _b = convert_ldicts_to_sdict(blocker_list)
    dot_graph(_b)


# def is_addidlast(blocker_list):
#     _b = convert_ldicts_to_sdict(blocker_list)
#     node = get_keys(dict(_b), 'add_id')[0]
#     if len(get_dependents(dict(_b), node)):
#         return False
#     return True

def get_lastnode(d):
    for key in d.keys():
        if len(get_dependents(d, key)) == 0:
            return key

def comp(dag, blocker_list):
    from copy import deepcopy
    dag = deepcopy(dag)
    _b = convert_ldicts_to_sdict(blocker_list)

    last_node = get_lastnode(dict(_b))
    if last_node != dag.key:
        dag = Delayed(last_node, _b)
    x = dag.compute()
    return x
