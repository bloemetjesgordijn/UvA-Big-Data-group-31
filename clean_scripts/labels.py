import duckdb
import pandas as pd
import os

if __name__ == '__main__':
    ### Connect to DB
    conn = duckdb.connect(os.getcwd() + '/db/db.duckdb', read_only=True)

    ### File specific variables
    train_col = "label"
    duckDB_col = "labels"

    ### Standard creation of relevant dataframes
    train_frames = []

    for i in conn.execute('PRAGMA show_tables').fetchdf()['name'].tolist():
        if i.startswith('train'):
            train_frames.append(conn.execute(f"SELECT tconst, {train_col} FROM {i}").fetchdf())
       
    full_train_df = pd.concat(train_frames)
    tconst_train = full_train_df['tconst'].tolist()
    ###################

    ### Cleaning/adjusting datatrain_end_year = []
    labels = full_train_df['label'].tolist()
    train_labels = []
    for i in range(len(labels)):
        train_labels.append([tconst_train[i], labels[i]])


    ### Creating new tables in DB
    conn.close()
    conn = duckdb.connect(os.getcwd() + '/db/db.duckdb', read_only=False)
    for i in ['train']:
        curr = f"{duckDB_col}_{i}"
        try:
            conn.execute(f"DROP TABLE {curr}")
        except:
            print(f"   {curr} does not exist yet.")
        try: 
            conn.execute(f"CREATE TABLE {curr}(tconst VARCHAR PRIMARY KEY, {duckDB_col} BOOLEAN)")
            conn.executemany(f"INSERT INTO {curr} VALUES (?, ?)", locals()[f"{i}_{duckDB_col}"])
            print(f"Created or updated {curr}")
        except Exception as e:
            print(f"   Could not create {curr} with error: {e}")