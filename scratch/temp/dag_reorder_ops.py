# coding=utf-8
from dmagellan.utils.py_utils.sample_fns import *
from collections import deque
from copy import deepcopy

# C = pd.DataFrame()
# D = attr_block_candset(C, 'birth_year', 'birth_year', 1)
# E = overlap_block_candset(D, 'address', 'address', 1)
# F = attr_block_candset(E, 'year', 'year', 1)

def fn():
    pass
dag = {
    'attr-1':[fn, pd.DataFrame()],
    'bb-1':[fn, 'attr-1'],
    'overlap-1':[fn, 'bb-1'],
    'attr-2':[fn, 'overlap-1']
}

###################

def list_diff(list1, list2):
    return [x for x in list1 if x not in list2]


def get_input_to_key(dag, key):
    return dag[key][1]


def get_keys_with_input(dag, key, head, tail):
    q = []
    keys= []
    condition = True
    q.append(tail)
    while condition:
        cur_node = q.pop(0)
        k = get_input_to_key(dag, cur_node)
        if type(k) == type(''):
            if k == key:
                keys.append(cur_node)
            if k != head:
                q.append(k)
        if len(q) == 0:
            condition = False
    return keys


def swap(dag, key1, key2, head, tail):
    d1 = deepcopy(dag)
    k1 = get_input_to_key(dag, key1)
    k2 = get_input_to_key(dag, key2)
    if key1 == k2:
        d1[key1][1] = key2
    else:
        d1[key1][1] = k2

    if key2 == k1:
        d1[key2][1] = key1
    else:
        d1[key2][1] = k1
    node1_input_to = get_keys_with_input(dag, key1, head, tail)
    node1_input_to = list_diff(node1_input_to, [key2])

    for key in node1_input_to:
        d1[key][1] = key2

    node2_input_to = get_keys_with_input(dag, key2, head, tail)
    node2_input_to = list_diff(node2_input_to, [key1])
    for key in node2_input_to:
        d1[key][1] = key1
    return d1


###################
res = swap(dag, 'attr-1', 'attr-2', 'attr-1', 'attr-2')

print(res)