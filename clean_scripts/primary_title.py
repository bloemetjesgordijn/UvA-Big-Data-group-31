import duckdb
import unidecode
import pandas as pd
import os

if __name__ == '__main__':
    ### Connect to DB
    conn = duckdb.connect(os.getcwd() + '/db/db.duckdb', read_only=True)

    ### File specific variables
    train_col = "primaryTitle"
    duckDB_col = "primary_title"

    ### Standard creation of relevant dataframes
    train_frames = []
    test_frames = []
    validation_frames = []
    for i in conn.execute('PRAGMA show_tables').fetchdf()['name'].tolist():
        if i.startswith('train'):
            train_frames.append(conn.execute(f"SELECT tconst, {train_col} FROM {i}").fetchdf())
        elif i.startswith('test'):
            test_frames.append(conn.execute(f"SELECT tconst, {train_col} FROM {i}").fetchdf())
        elif i.startswith('validation'):
            validation_frames.append(conn.execute(f"SELECT tconst, {train_col} FROM {i}").fetchdf())
    
    full_train_df = pd.concat(train_frames)
    full_test_df = pd.concat(test_frames)
    full_validation_df = pd.concat(validation_frames)

    tconst_train = full_train_df['tconst'].tolist()
    tconst_test = full_test_df['tconst'].tolist()
    tconst_validation = full_validation_df['tconst'].tolist()
    ###################

    ### Cleaning/adjusting data
    train_primary_title = []
    for i in range(len(full_train_df)):
        train_primary_title.append([tconst_train[i], unidecode.unidecode(full_train_df.iloc[i]['primaryTitle'])])

    test_primary_title = []
    for i in range(len(full_test_df)):
        test_primary_title.append([tconst_test[i], unidecode.unidecode(full_test_df.iloc[i]['primaryTitle'])])

    validation_primary_title = []
    for i in range(len(full_validation_df)):
        validation_primary_title.append([tconst_validation[i], unidecode.unidecode(full_validation_df.iloc[i]['primaryTitle'])])

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
            conn.execute(f"CREATE TABLE {curr}(tconst VARCHAR PRIMARY KEY, {duckDB_col} VARCHAR)")
            conn.executemany(f"INSERT INTO {curr} VALUES (?, ?)", locals()[f"{i}_{duckDB_col}"])
            print(f"Created or updated {curr}")
        except Exception as e:
            print(f"   Could not create {curr} with error: {e}")    