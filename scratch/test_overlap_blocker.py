import os

import pandas as pd
import dask

from dmagellan.blocker.overlap.overlapblocker import OverlapBlocker

datapath = "../datasets/"
A = pd.read_csv(os.path.join(datapath, 'person_table_A.csv'), low_memory=False)
B = pd.read_csv(os.path.join(datapath, 'person_table_B.csv'), low_memory=False)

# A = pd.read_csv(os.path.join(datapath, 'tracks.csv'), low_memory=False)
# B = pd.read_csv(os.path.join(datapath, 'songs.csv'), low_memory=False)

print('Reading the files done')
ob = OverlapBlocker()
C = ob.block_tables(A, B, 'ID', 'ID', 'address', 'address',
                    l_output_attrs=['name', 'address','zipcode'],
                    r_output_attrs=['name', 'address', 'zipcode'],
                    overlap_size=2, nltable_chunks=2, nrtable_chunks=2,
                    compute=False, scheduler=dask.get
                    )
D = ob.block_candset(C, A, B, "l_ID", "r_ID", "ID", "ID", 'name', 'name',
                     nchunks=4, overlap_size=1, compute=True)
print(len(D))
# print(D.head())



