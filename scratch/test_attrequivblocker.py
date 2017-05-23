import os

import pandas as pd
import dask

from dmagellan.blocker.attrequivalence.attr_equiv_blocker import AttrEquivalenceBlocker

datapath = "../datasets/"
A = pd.read_csv(os.path.join(datapath, 'person_table_A.csv'), low_memory=False)
B = pd.read_csv(os.path.join(datapath, 'person_table_B.csv'), low_memory=False)

# A = pd.read_csv(os.path.join(datapath, 'tracks.csv'), low_memory=False)
# B = pd.read_csv(os.path.join(datapath, 'songs.csv'), low_memory=False)

print('Reading the files done')
ab = AttrEquivalenceBlocker()
C = ab.block_tables(A, B, 'ID', 'ID', 'birth_year', 'birth_year', ['name', 'address',
                                                                   'zipcode'],
                    ['name', 'address', 'zipcode'], nltable_chunks=2, nrtable_chunks=2,
                    compute=False, scheduler=dask.get
                    )
D = ab.block_candset(C, A, B, "l_ID", "r_ID", "ID", "ID", 'zipcode', 'zipcode',
                     nchunks=4, compute=True)
print(len(D))




