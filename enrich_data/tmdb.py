
import duckdb
import pandas as pd
import os
import requests
import json
import time


### This script queries a slow backend a shitload of times so it will take around 20 minutes.

if __name__ == '__main__':
    now = time.time()
    api_key = "d3bb55da5cf488e09157238216d2dba8"

    ### Connect to DB
    conn = duckdb.connect('../db/db.duckdb', read_only=True)

    ### File specific variables
    duckDB_col = "tmdb_ratings"

    tconst_train = conn.execute('SELECT tconst FROM labels_train').fetchdf()['tconst'].tolist()
    tconst_test = conn.execute('SELECT tconst FROM test_hidden').fetchdf()['tconst'].tolist()
    tconst_validation = conn.execute('SELECT tconst FROM validation_hidden').fetchdf()['tconst'].tolist()

    train_tmdb_ratings = []
    for i in tconst_train:
        try:
            response = requests.get(f"https://api.themoviedb.org/3/find/{i}?api_key={api_key}&language=en-US&external_source=imdb_id")
            response_string = response.content.decode('utf-8')
            response_json = json.loads(response_string)
            rating = float(response_json['movie_results'][0].get('vote_average'))
            train_tmdb_ratings.append([i, rating])
        except Exception as e:
            print(f"Error querying {i} with error: {e}")
            print(len(train_tmdb_ratings))

    print(len(train_tmdb_ratings))

    test_tmdb_ratings = []
    for i in tconst_test:
        try:
            response = requests.get(f"https://api.themoviedb.org/3/find/{i}?api_key={api_key}&language=en-US&external_source=imdb_id")
            response_string = response.content.decode('utf-8')
            response_json = json.loads(response_string)
            rating = float(response_json['movie_results'][0].get('vote_average'))
            test_tmdb_ratings.append([i, rating])
        except Exception as e:
            print(f"Error querying {i} with error: {e}")
            print(len(test_tmdb_ratings))

    print(len(test_tmdb_ratings))

    validation_tmdb_ratings = []
    for i in tconst_validation:
        try:
            response = requests.get(f"https://api.themoviedb.org/3/find/{i}?api_key={api_key}&language=en-US&external_source=imdb_id")
            response_string = response.content.decode('utf-8')
            response_json = json.loads(response_string)
            rating = float(response_json['movie_results'][0].get('vote_average'))
            validation_tmdb_ratings.append([i, rating])
        except Exception as e:
            print(f"Error querying {i} with error: {e}")
            print(len(validation_tmdb_ratings))

    print(len(validation_tmdb_ratings))
    print(time.time() - now)

    ### Creating new tables in DB
    conn.close()
    conn = duckdb.connect('../db/db.duckdb', read_only=False)
    for i in ['train', 'test', 'validation']:
        curr = f"{duckDB_col}_{i}"
        try:
            conn.execute(f"DROP TABLE {curr}")
        except:
            print(f"   {curr} does not exist yet.")
        try: 
            conn.execute(f"CREATE TABLE {curr}(tconst VARCHAR PRIMARY KEY, {duckDB_col} FLOAT)")
            conn.executemany(f"INSERT INTO {curr} VALUES (?, ?)", locals()[f"{i}_{duckDB_col}"])
            print(f"Created or updated {curr}")
        except Exception as e:
            print(f"   Could not create {curr} with error: {e}")