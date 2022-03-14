import os
import duckdb

import pandas as pd


if __name__ == '__main__':
    # connect or create
    conn = duckdb.connect('db.duckdb', read_only=False)

    # set path for data
    data_root = os.path.join('..', 'imdb')

    for fname in os.listdir(data_root):
        if fname.startswith('train'):
            # load train csv in pandas df
            path = os.path.join(data_root, fname)
            df = pd.read_csv(path)

            # register and create in duckdb
            new_name = ''.join(fname.split('.')[0].split('-'))
            conn.register(new_name, df)
            conn.execute(f'CREATE TABLE {new_name} AS SELECT * FROM {new_name}')