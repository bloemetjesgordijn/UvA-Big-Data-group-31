import os
import duckdb

if __name__ == '__main__':
    # connect
    db_path = os.path.join('db', 'db.duckdb')
    conn = duckdb.connect(db_path, read_only=True)

    # execute
    df = conn.execute('SELECT * FROM train').fetchdf()
    print(df)
