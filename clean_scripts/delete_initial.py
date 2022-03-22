import duckdb
import os

if __name__ == '__main__':
    ### Connect to DB
    conn = duckdb.connect(os.getcwd() + '/db/db.duckdb', read_only=False)
    for i in conn.execute('PRAGMA show_tables').fetchdf()['name'].tolist():
        if i.startswith('train') or i.startswith('test') or i.startswith('validation'):
            conn.execute(f"DROP TABLE {i}")
            print(f"Deleted {i}")