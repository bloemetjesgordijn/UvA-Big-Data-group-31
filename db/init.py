import os
import duckdb
import requests

import pandas as pd

from io import StringIO


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
        
        elif fname.endswith('.json'):
            # load json in pandas df
            df = pd.read_json(path)

            # register and create in duckdb
            new_name = fname.replace('.json', '')
            conn.register(new_name, df)

            conn.execute(f'CREATE TABLE {new_name} AS SELECT * FROM {new_name}')

    # get genres
    external = 'https://raw.githubusercontent.com/sidooms/MovieTweetings/master/latest/movies.dat'
    response = requests.get(external)

    df = pd.read_table(StringIO(response.content.decode('utf-8')), sep='::', engine='python', names=['tconst', 'title', 'genre'])

    conn.register('genres', df)
    conn.execute('CREATE TABLE genres AS SELECT * FROM genres')

    conn.close()
            