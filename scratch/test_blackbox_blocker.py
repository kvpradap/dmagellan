# coding=utf-8
import os

import pandas as pd
import dask
from dask.multiprocessing import get

from dmagellan.blocker.blackbox.blackbox_blocker import BlackBoxBlocker
from dmagellan.blocker.attrequivalence.attr_equiv_blocker import AttrEquivalenceBlocker


datapath = "../datasets/"
A = pd.read_csv(os.path.join(datapath, 'person_table_A.csv'), low_memory=False)
B = pd.read_csv(os.path.join(datapath, 'person_table_B.csv'), low_memory=False)

def last_name_match(ltuple, rtuple):
    l_first_name, l_last_name = ltuple['name'].split()
    r_first_name, r_last_name = rtuple['name'].split()
    return l_last_name != r_last_name

def zipcode_match(ltuple, rtuple):
    return ltuple['zipcode'] != rtuple['zipcode']


# A = pd.read_csv(os.path.join(datapath, 'tracks.csv'), low_memory=False)
# B = pd.read_csv(os.path.join(datapath, 'songs.csv'), low_memory=False)

print('Reading the files done')
ab = AttrEquivalenceBlocker()
bb = BlackBoxBlocker()
bb.set_black_box_function(last_name_match)
bb.set_ltable_attrs(['name'])
bb.set_rtable_attrs(['name'])

A['ID'] = A['ID'] + 10
C = bb.block_tables(A, B, 'ID', 'ID',
                    l_output_attrs=['name', 'address'],
                    r_output_attrs=['name', 'address'],
                    compute=False
                    )
D = bb.block_candset(C, A, B, 'l_ID', 'r_ID', "ID", "ID", nchunks=4,
                    compute=True, scheduler=get)

print(len(D))




# D = bb.block_candset(C, A, B, "l_ID", "r_ID", "ID", "ID", 'zipcode', 'zipcode',
#                      nchunks=4, compute=True)
# print(len(D))
# print(D.head())



