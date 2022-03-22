import duckdb
import pandas as pd
import os

if __name__ == '__main__':
    ### Connect to DB
    conn = duckdb.connect(os.getcwd() + '/db/db.duckdb', read_only=True)

    ### File specific variables
    duckDB_col = "user_ratings"

    ### Standard creation of relevant dataframes
    with open(os.getcwd() + "/movietweetings/ratings.dat", "r") as a:
        ratings = a.read().splitlines()
    ## This takes 2 minutes.
    ratings_tconst_list = []
    ratings_ratings_list = []
    for i in ratings:
        split_item = i.split('::')
        curr_tconst = "tt" + split_item[1]
        rating = split_item[2]
        ratings_tconst_list.append(curr_tconst)
        ratings_ratings_list.append(int(rating))
    ratings_df = pd.DataFrame(list(zip(ratings_tconst_list, ratings_ratings_list)), columns =['tconst', 'rating'])
    mean_ratings_df = ratings_df.groupby(['tconst']).mean()

    tconst_train = conn.execute('SELECT tconst FROM labels_train').fetchdf()['tconst'].tolist()
    tconst_test = conn.execute('SELECT tconst FROM test_hidden').fetchdf()['tconst'].tolist()
    tconst_validation = conn.execute('SELECT tconst FROM validation_hidden').fetchdf()['tconst'].tolist()
    ###################

    ## Cleaning/adjusting data
    mean_ratings_list = mean_ratings_df['rating'].tolist()
    mean_ratings_tconsts = mean_ratings_df.index.tolist()
    train_user_ratings = []
    for i in range(len(mean_ratings_list)):
        if mean_ratings_tconsts[i] in tconst_train:
            train_user_ratings.append([mean_ratings_tconsts[i], mean_ratings_list[i]])

    test_user_ratings = []
    for i in range(len(mean_ratings_list)):
        if mean_ratings_tconsts[i] in tconst_test:
            test_user_ratings.append([mean_ratings_tconsts[i], mean_ratings_list[i]])
    
    validation_user_ratings = []
    for i in range(len(mean_ratings_list)):
        if mean_ratings_tconsts[i] in tconst_validation:
            validation_user_ratings.append([mean_ratings_tconsts[i], mean_ratings_list[i]])

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