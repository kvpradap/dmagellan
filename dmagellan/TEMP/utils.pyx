def get_str_cols(dataframe):
    return dataframe.columns[dataframe.dtypes == 'object']

def str2bytes(x):
    if isinstance(x, bytes):
        return x
    else:
        return x.encode('utf-8')
