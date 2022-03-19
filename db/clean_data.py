import duckdb
import math
import unidecode
import pandas as pd



if __name__ == '__main__':
    conn = duckdb.connect('db.duckdb', read_only=False)
    # Create dataframe from all tables in the db
    frames = []
    for i in conn.execute('PRAGMA show_tables').fetchdf()['name'].tolist():
        frames.append(conn.execute(f"SELECT * FROM {i}").fetchdf())
    full_train_df = pd.concat(frames)

    #Create cleaned single lists for every column
    tconst_list = full_train_df['tconst'].tolist()

    primary_title = []
    for i in range(len(full_train_df)):
        primary_title.append(unidecode.unidecode(full_train_df.iloc[i]['primaryTitle']))

    original_title = []
    for i in range(len(full_train_df)):
        curr = full_train_df.iloc[i]['originalTitle']
        if isinstance(curr, str):
            original_title.append(unidecode.unidecode(curr))
        else:
            original_title.append('')

    start_year = []
    for i in range(len(full_train_df)):
        curr = full_train_df.iloc[i]['startYear']
        if curr == "\\N":
            start_year.append(int())
        else:
            start_year.append(int(curr))
    start_year = [x for x in start_year if math.isnan(x) == False]

    end_year = []
    for i in range(len(full_train_df)):
        curr = full_train_df.iloc[i]['endYear']
        if curr == "\\N":
            end_year.append(int())
        else:
            end_year.append(int(curr))
    end_year = [x for x in end_year if math.isnan(x) == False]

    runtime_minutes = []
    for i in range(len(full_train_df)):
        curr = full_train_df.iloc[i]['runtimeMinutes']
        if curr == "\\N":
            runtime_minutes.append(int())
        else:
            runtime_minutes.append(int(curr))
    runtime_minutes = [x for x in runtime_minutes if math.isnan(x) == False]

    num_votes = []
    for i in range(len(full_train_df)):
        curr = full_train_df.iloc[i]['numVotes']
        if curr == 'nan' or curr == "NaN" or math.isnan(curr):
            num_votes.append(int())
        else:
            num_votes.append(int(curr))
    num_votes = [x for x in num_votes if math.isnan(x) == False]

    labels = full_train_df['label'].tolist()

    # Remove all train tables from the database
    for i in conn.execute('PRAGMA show_tables').fetchdf()['name'].tolist():
        if i.startswith('train'):
            conn.execute(f"DROP VIEW {i}")


    # Create new tables based on created single cleaned lists

    curr = "primary_title"
    print(f"Creating {curr} table")
    try:
        conn.execute('DROP TABLE primary_title')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute('CREATE TABLE primary_title(tconst_list INTEGER PRIMARY KEY, primary_title VARCHAR)')
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")    

    curr = "original_title"
    print(f"Creating {curr} table")
    try:
        conn.execute('DROP TABLE original_title')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute('CREATE TABLE original_title(tconst_list INTEGER PRIMARY KEY, original_title VARCHAR)')
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")   
        
    curr = "start_year"
    print(f"Creating {curr} table")
    try:
        conn.execute('DROP TABLE start_year')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute('CREATE TABLE start_year(tconst_list INTEGER PRIMARY KEY, start_year INTEGER)')
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")
        
    curr = "end_year"
    print(f"Creating {curr} table")
    try:
        conn.execute('DROP TABLE end_year')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute('CREATE TABLE end_year(tconst_list INTEGER PRIMARY KEY, end_year INTEGER)')
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")
        
    curr = "runtime"
    print(f"Creating {curr} table")
    try:
        conn.execute('DROP TABLE runtime')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute('CREATE TABLE runtime(tconst_list INTEGER PRIMARY KEY, runtime_minutes INTEGER)')
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")

    curr = "num_votes"
    print(f"Creating {curr} table")
    try:
        conn.execute('DROP TABLE num_votes')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute('CREATE TABLE num_votes(tconst_list INTEGER PRIMARY KEY, num_votes INTEGER)')
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")
        
    curr = "labels"
    print(f"Creating {curr} table")
    try:
        conn.execute('DROP TABLE labels')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute('CREATE TABLE labels(tconst_list INTEGER PRIMARY KEY, labels BOOLEAN)')
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")