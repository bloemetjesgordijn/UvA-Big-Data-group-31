import os
import duckdb

import pandas as pd


if __name__ == '__main__':
    # connect or create
    conn = duckdb.connect('db.duckdb', read_only=False)

    # set path for data
    data_root = os.path.join('..', 'imdb')

    count = 0
    for fname in os.listdir(data_root):
        path = os.path.join(data_root, fname)
        
        if fname.startswith('train'):
            # load train csv in pandas df
            df = pd.read_csv(path)

            # register and create in duckdb
            new_name = ''.join(fname.split('.')[0].split('-'))
            conn.register(new_name, df)

            if count == 0:
                conn.execute(f'CREATE TABLE train AS SELECT * FROM {new_name}')
            else:
                conn.execute(f'INSERT INTO train SELECT * FROM {new_name}')

            count += 1
            