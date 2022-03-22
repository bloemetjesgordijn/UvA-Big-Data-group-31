import duckdb
import pandas as pd
import os

if __name__ == '__main__':

    ### File specific variables
    duckDB_col = "movielens_ratings"

    ratings_tconst_list = []
    ratings_ratings_list = []
    movielens_df = pd.read_csv(os.getcwd() + '/movielens/ratings.csv')
    for i in range(len(movielens_df)):
        curr = movielens_df.iloc[i]
        curr_id = str(int(curr['movieId']))
        zeroes_to_add = 7 - len(curr_id)
        for i in range(zeroes_to_add):
            curr_id = "0" + curr_id
        curr_tconst = f"tt{curr_id}"
        ratings_ratings_list.append(float(curr['rating']))
        ratings_tconst_list.append(curr_tconst)
    ratings_df = pd.DataFrame(list(zip(ratings_tconst_list, ratings_ratings_list)), columns =['tconst', 'rating'])
    mean_ratings_df = ratings_df.groupby(['tconst']).mean()


    tconst_train = conn.execute('SELECT tconst FROM labels_train').fetchdf()['tconst'].tolist()
    tconst_test = conn.execute('SELECT tconst FROM test_hidden').fetchdf()['tconst'].tolist()
    tconst_validation = conn.execute('SELECT tconst FROM validation_hidden').fetchdf()['tconst'].tolist()

## Cleaning/adjusting data
    mean_ratings_list = mean_ratings_df['rating'].tolist()
    mean_ratings_tconsts = mean_ratings_df.index.tolist()
    train_movielens_ratings = []
    for i in range(len(mean_ratings_list)):
        if mean_ratings_tconsts[i] in tconst_train:
            train_movielens_ratings.append([mean_ratings_tconsts[i], mean_ratings_list[i]])
    print(len(train_movielens_ratings))
    test_movielens_ratings = []
    for i in range(len(mean_ratings_list)):
        if mean_ratings_tconsts[i] in tconst_test:
            test_movielens_ratings.append([mean_ratings_tconsts[i], mean_ratings_list[i]])
    print(len(test_movielens_ratings))
    validation_movielens_ratings = []
    for i in range(len(mean_ratings_list)):
        if mean_ratings_tconsts[i] in tconst_validation:
            validation_movielens_ratings.append([mean_ratings_tconsts[i], mean_ratings_list[i]])
    print(len(validation_movielens_ratings))
    ### Creating new tables in DB
    conn.close()
    conn = duckdb.connect(os.getcwd() + '/db/db.duckdb', read_only=False)
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