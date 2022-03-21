import duckdb
import math
import os
import unidecode
import pandas as pd


def clean_and_send(full_train_df, tconst, set_type):

    with open("../movietweetings/ratings.dat", "r") as a:
        ratings = a.read().splitlines()
    ## This takes 2 minutes.
    ratings_tconst_list = []
    ratings_ratings_list = []
    for i in ratings:
        split_item = i.split('::')
        curr_tconst = "tt" + split_item[1]
        if curr_tconst in tconst:
            rating = split_item[2]
            ratings_tconst_list.append(curr_tconst)
            ratings_ratings_list.append(int(rating))

    ratings_df = pd.DataFrame(list(zip(ratings_tconst_list, ratings_ratings_list)), columns =['tconst', 'rating'])
    mean_ratings_df = ratings_df.groupby(['tconst']).mean()


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

    mean_ratings_list = mean_ratings_df['rating'].tolist()
    mean_ratings_tconsts = mean_ratings_df.index.tolist()
    ratings_list = []
    for i in range(len(mean_ratings_list)):
        ratings_list.append([mean_ratings_tconsts[i], mean_ratings_list[i]])

    if set_type == 'train':
        labels = full_train_df['label'].tolist()
        label_list = []
        for i in range(len(labels)):
            label_list.append([tconst[i], labels[i]])

    # Remove all train tables from the database
    for i in conn.execute('PRAGMA show_tables').fetchdf()['name'].tolist():
        if i.startswith('train'):
            conn.execute(f"DROP TABLE {i}")
    # Remove test_hidden and test_validation tables from the database
    if set_type == 'test':
        conn.execute(f"DROP TABLE test_hidden")
    if set_type == 'validation':
        conn.execute(f"DROP TABLE validation_hidden")

    # Create new tables based on created single cleaned lists
    if set_type == 'train':
        curr = "primary_title"
    elif set_type == 'test':
        curr = "test_primary_title"
    elif set_type == 'validation':
        curr = "validation_primary_title"
    print(f"Creating {curr} table")
    try:
        conn.execute(f'DROP TABLE {curr}')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute(f"CREATE TABLE {curr}(tconst VARCHAR PRIMARY KEY, primary_title VARCHAR)")
        conn.executemany(f"INSERT INTO {curr} VALUES (?, ?)", primary_title)

    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")    
    
    if set_type == 'train':
        curr = "original_title"
    elif set_type == 'test':
        curr = "test_original_title"
    elif set_type == 'validation':
        curr = "validation_original_title"
    print(f"Creating {curr} table")
    try:
        conn.execute(f'DROP TABLE {curr}')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute(f'CREATE TABLE {curr}(tconst VARCHAR PRIMARY KEY, original_title VARCHAR)')
        conn.executemany(f"INSERT INTO {curr} VALUES (?, ?)", original_title)
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")   
    
    if set_type == 'train':
        curr = "start_year"
    elif set_type == 'test': 
        curr = "test_start_year"
    elif set_type == 'validation':
        curr = 'validation_start_year'
    print(f"Creating {curr} table")
    try:
        conn.execute(f'DROP TABLE {curr}')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute(f'CREATE TABLE {curr}(tconst VARCHAR PRIMARY KEY, start_year INTEGER)')
        conn.executemany(f"INSERT INTO {curr} VALUES (?, ?)", start_year)
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")
        
    if set_type == 'train':
        curr = "end_year"
    elif set_type == 'test':
        curr = "test_end_year"
    elif set_type == 'validation':
        curr = "validation_end_year"
    print(f"Creating {curr} table")
    try:
        conn.execute(f'DROP TABLE {curr}')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute(f'CREATE TABLE {curr}(tconst VARCHAR PRIMARY KEY, end_year INTEGER)')
        conn.executemany(f"INSERT INTO {curr} VALUES (?, ?)", end_year)
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")
        
    if set_type == 'train':
        curr = "runtime"
    elif set_type == 'test':
        curr = "test_runtime"
    elif set_type == 'validation':
        curr = "validation_runtime"
    print(f"Creating {curr} table")
    try:
        conn.execute(f'DROP TABLE {curr}')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute(f'CREATE TABLE {curr}(tconst VARCHAR PRIMARY KEY, runtime_minutes INTEGER)')
        conn.executemany(f"INSERT INTO {curr} VALUES (?, ?)", runtime_minutes)
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")

    if set_type == 'train':
        curr = "num_votes"
    elif set_type == 'test':
        curr = "test_num_votes"
    elif set_type == 'validation':
        curr = "validation_num_votes"
    print(f"Creating {curr} table")
    try:
        conn.execute(f'DROP TABLE {curr}')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute(f'CREATE TABLE {curr}(tconst VARCHAR PRIMARY KEY, num_votes INTEGER)')
        conn.executemany(f"INSERT INTO {curr} VALUES (?, ?)", num_votes)
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")
    
    if set_type == 'train':
        curr = "labels"
        print(f"Creating {curr} table")
        try:
            conn.execute(f'DROP TABLE {curr}')
        except:
            print(f"   {curr} did not exist")
        try:
            conn.execute(f'CREATE TABLE {curr}(tconst VARCHAR PRIMARY KEY, labels BOOLEAN)')
            conn.executemany(f"INSERT INTO {curr} VALUES (?, ?)", label_list)
        except Exception as e:
            print(f"   Could not create {curr} with error: {e}")

    if set_type == 'train':
        curr = "user_ratings"
    elif set_type == 'test':
        curr = "test_user_ratings"
    elif set_type == "validation":
        curr = "validation_user_ratings"
    print(f"Creating {curr} table")
    try:
        conn.execute(f'DROP TABLE {curr}')
    except:
        print(f"   {curr} did not exist")
    try:
        conn.execute(f'CREATE TABLE {curr}(tconst VARCHAR PRIMARY KEY, rating FLOAT)')
        conn.executemany(f"INSERT INTO {curr} VALUES (?, ?)", ratings_list)
    except Exception as e:
        print(f"   Could not create {curr} with error: {e}")


if __name__ == '__main__':
    conn = duckdb.connect('db.duckdb', read_only=False)
    # Create dataframe from all tables in the db
    frames = []
    test_frames = []
    validation_frames = []
    print(conn.execute('PRAGMA show_tables').fetchdf()['name'].tolist())
    for i in conn.execute('PRAGMA show_tables').fetchdf()['name'].tolist():
        if i.startswith('train'):
            frames.append(conn.execute(f"SELECT * FROM {i}").fetchdf())
        elif i.startswith('test'):
            test_frames.append(conn.execute(f"SELECT * FROM {i}").fetchdf())
        elif i.startswith('validation'):
            validation_frames.append(conn.execute(f"SELECT * FROM {i}").fetchdf())
    full_train_df = pd.concat(frames)
    full_test_df = pd.concat(test_frames)
    full_validation_df = pd.concat(validation_frames)

    #Create cleaned single lists for every column
    tconst_train = full_train_df['tconst'].tolist()
    tconst_test = full_test_df['tconst'].tolist()
    tconst_validation = full_validation_df['tconst'].tolist()

    clean_and_send(full_validation_df, tconst_validation, 'validation')
    clean_and_send(full_test_df, tconst_test, 'test')
    clean_and_send(full_train_df, tconst_train, 'train')


