import duckdb
import math
import os
import unidecode
import pandas as pd



if __name__ == '__main__':
    print(os.getcwd())
    conn = duckdb.connect('db.duckdb', read_only=False)
    # Create dataframe from all tables in the db
    frames = []
    print(conn.execute('PRAGMA show_tables').fetchdf()['name'].tolist())
    for i in conn.execute('PRAGMA show_tables').fetchdf()['name'].tolist():
        frames.append(conn.execute(f"SELECT * FROM {i}").fetchdf())
    full_train_df = pd.concat(frames)

    #Create cleaned single lists for every column
    tconst = full_train_df['tconst'].tolist()

    primary_title = []
    for i in range(len(full_train_df)):
        primary_title.append([tconst[i], unidecode.unidecode(full_train_df.iloc[i]['primaryTitle'])])

    original_title = []
    for i in range(len(full_train_df)):
        curr = full_train_df.iloc[i]['originalTitle']
        curr_primary_title = full_train_df.iloc[i]['primaryTitle']
        if isinstance(curr, str):
            original_title.append([tconst[i], unidecode.unidecode(curr)])
        else:
            original_title.append([tconst[i], ''])

    start_year = []
    for i in range(len(full_train_df)):
        curr = full_train_df.iloc[i]['startYear']
        if curr == "\\N":
            start_year.append([tconst[i], int()])
        else:
            start_year.append([tconst[i], int(curr)])

    end_year = []
    for i in range(len(full_train_df)):
        curr = full_train_df.iloc[i]['endYear']
        if curr == "\\N":
            end_year.append([tconst[i], int()])
        else:
            end_year.append([tconst[i], int(curr)])

    runtime_minutes = []
    for i in range(len(full_train_df)):
        curr = full_train_df.iloc[i]['runtimeMinutes']
        if curr == "\\N":
            runtime_minutes.append([tconst[i], int()])
        else:
            runtime_minutes.append([tconst[i], int(curr)])

    num_votes = []
    for i in range(len(full_train_df)):
        curr = full_train_df.iloc[i]['numVotes']
        if curr == 'nan' or curr == "NaN" or math.isnan(curr):
            num_votes.append([tconst[i], int()])
        else:
            num_votes.append([tconst[i], int(curr)])

    labels = full_train_df['label'].tolist()
    label_list = []
    for i in range(len(labels)):
        label_list.append([tconst[i], labels[i]])

    # Remove all train tables from the database
    for i in conn.execute('PRAGMA show_tables').fetchdf()['name'].tolist():
        if i.startswith('train'):
            conn.execute(f"DROP TABLE {i}")


    # Create new tables based on created single cleaned lists

    curr = "primary_title"
    print(f"Creating {curr} table")
    try:
        conn.execute('DROP TABLE primary_title')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute('CREATE TABLE primary_title(tconst VARCHAR PRIMARY KEY, primary_title VARCHAR)')
        conn.executemany("INSERT INTO primary_title VALUES (?, ?)", primary_title)

    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")    

    curr = "original_title"
    print(f"Creating {curr} table")
    try:
        conn.execute('DROP TABLE original_title')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute('CREATE TABLE original_title(tconst VARCHAR PRIMARY KEY, original_title VARCHAR)')
        conn.executemany("INSERT INTO original_title VALUES (?, ?)", original_title)
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")   
        
    curr = "start_year"
    print(f"Creating {curr} table")
    try:
        conn.execute('DROP TABLE start_year')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute('CREATE TABLE start_year(tconst VARCHAR PRIMARY KEY, start_year INTEGER)')
        conn.executemany("INSERT INTO start_year VALUES (?, ?)", start_year)
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")
        
    curr = "end_year"
    print(f"Creating {curr} table")
    try:
        conn.execute('DROP TABLE end_year')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute('CREATE TABLE end_year(tconst VARCHAR PRIMARY KEY, end_year INTEGER)')
        conn.executemany("INSERT INTO end_year VALUES (?, ?)", end_year)
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")
        
    curr = "runtime"
    print(f"Creating {curr} table")
    try:
        conn.execute('DROP TABLE runtime')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute('CREATE TABLE runtime(tconst VARCHAR PRIMARY KEY, runtime_minutes INTEGER)')
        conn.executemany("INSERT INTO runtime VALUES (?, ?)", runtime_minutes)
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")

    curr = "num_votes"
    print(f"Creating {curr} table")
    try:
        conn.execute('DROP TABLE num_votes')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute('CREATE TABLE num_votes(tconst VARCHAR PRIMARY KEY, num_votes INTEGER)')
        conn.executemany("INSERT INTO num_votes VALUES (?, ?)", num_votes)
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")
        e
    curr = "labels"
    print(f"Creating {curr} table")
    try:
        conn.execute('DROP TABLE labels')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute('CREATE TABLE labels(tconst VARCHAR PRIMARY KEY, labels BOOLEAN)')
        conn.executemany("INSERT INTO labels VALUES (?, ?)", label_list)
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")