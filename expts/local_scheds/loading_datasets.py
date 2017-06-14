import pandas as pd
import psutil

print('Before: {}'.format(psutil.virtual_memory().used/1e9))
A = pd.read_csv('./datasets/sample_citeseer_300k.csv')
B = pd.read_csv('./datasets/sample_dblp_300k.csv')
print('After: {}'.format(psutil.virtual_memory().used/1e9))

