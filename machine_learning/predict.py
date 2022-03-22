import duckdb
import pandas as pd
from functools import reduce
import numpy as np
import os
from pathlib import Path
from sklearn.preprocessing import StandardScaler
import keras
from keras.models import Sequential   # importing Sequential model
from keras.layers import Dense        # importing Dense layers
import keras.optimizers
import tensorflow as tf
import time

if __name__ == '__main__':
    conn = duckdb.connect(os.getcwd() + '/db/db.duckdb', read_only=True)

    test_merged_df = conn.execute('''SELECT end_year_test.tconst, end_year_test.end_year, num_votes_test.num_votes, original_title_test.original_title, primary_title_test.primary_title, runtime_minutes_test.runtime_minutes, start_year_test.start_year, user_ratings_test.user_ratings  
                         FROM end_year_test 
                         INNER JOIN num_votes_test ON end_year_test.tconst = num_votes_test.tconst
                         INNER JOIN original_title_test ON end_year_test.tconst = original_title_test.tconst
                         INNER JOIN primary_title_test ON end_year_test.tconst = primary_title_test.tconst
                         INNER JOIN runtime_minutes_test ON end_year_test.tconst = runtime_minutes_test.tconst
                         INNER JOIN start_year_test ON end_year_test.tconst = start_year_test.tconst
                         FULL OUTER JOIN user_ratings_test ON end_year_test.tconst = user_ratings_test.tconst
                         ''').fetchdf()

    tconst_order = conn.execute('SELECT tconst FROM end_year_test').fetchdf()
    tconst_order['order'] = range(len(tconst_order))
    test_merged_df = test_merged_df.merge(tconst_order, on='tconst').sort_values(by=['order']).drop('order', axis=1)

    validation_merged_df = conn.execute('''SELECT end_year_validation.tconst, end_year_validation.end_year, num_votes_validation.num_votes, original_title_validation.original_title, primary_title_validation.primary_title, runtime_minutes_validation.runtime_minutes, start_year_validation.start_year, user_ratings_validation.user_ratings  
                         FROM end_year_validation 
                         INNER JOIN num_votes_validation ON end_year_validation.tconst = num_votes_validation.tconst
                         INNER JOIN original_title_validation ON end_year_validation.tconst = original_title_validation.tconst
                         INNER JOIN primary_title_validation ON end_year_validation.tconst = primary_title_validation.tconst
                         INNER JOIN runtime_minutes_validation ON end_year_validation.tconst = runtime_minutes_validation.tconst
                         INNER JOIN start_year_validation ON end_year_validation.tconst = start_year_validation.tconst
                         FULL OUTER JOIN user_ratings_validation ON end_year_validation.tconst = user_ratings_validation.tconst
                         ''').fetchdf()

    tconst_order = conn.execute('SELECT tconst FROM end_year_validation').fetchdf()
    tconst_order['order'] = range(len(tconst_order))
    validation_merged_df = validation_merged_df.merge(tconst_order, on='tconst').sort_values(by=['order']).drop('order', axis=1)

    renamed = []
    for i in range(len(test_merged_df)):
        curr_original = test_merged_df.iloc[i]['original_title']
        curr_primary = test_merged_df.iloc[i]['primary_title']
        if curr_original != "" and curr_primary != curr_original:
            renamed.append(1)
        else:
            renamed.append(0)
        
    test_merged_df['renamed'] = renamed

    renamed = []
    for i in range(len(validation_merged_df)):
        curr_original = validation_merged_df.iloc[i]['original_title']
        curr_primary = validation_merged_df.iloc[i]['primary_title']
        if curr_original != "" and curr_primary != curr_original:
            renamed.append(1)
        else:
            renamed.append(0)
        
    validation_merged_df['renamed'] = renamed

    test_merged_df = test_merged_df.drop('original_title', 1)
    test_merged_df = test_merged_df.drop('primary_title', 1)
    test_merged_df = test_merged_df.drop('tconst', 1)

    validation_merged_df = validation_merged_df.drop('original_title', 1)
    validation_merged_df = validation_merged_df.drop('primary_title', 1)
    validation_merged_df = validation_merged_df.drop('tconst', 1)

    test_with_rating_index = np.where(test_merged_df['user_ratings'].notnull())[0]
    test_without_rating_index = np.where(test_merged_df['user_ratings'].isnull())[0]

    validation_with_rating_index = np.where(validation_merged_df['user_ratings'].notnull())[0]
    validation_without_rating_index = np.where(validation_merged_df['user_ratings'].isnull())[0]

    # print(f"{len(test_with_rating_index)} + {len(test_without_rating_index)} = {len(test_merged_df)}")
    # print(f"{len(validation_with_rating_index)} + {len(validation_without_rating_index)} = {len(validation_merged_df)}")


    test_with_rating = test_merged_df.iloc[test_with_rating_index]
    test_without_rating = test_merged_df.iloc[test_without_rating_index]
    test_without_rating = test_without_rating.drop('user_ratings', 1)
    # print(f"{len(test_with_rating)} + {len(test_without_rating)} = {len(test_merged_df)}")

    validation_with_rating = validation_merged_df.iloc[validation_with_rating_index]
    validation_without_rating = validation_merged_df.iloc[validation_without_rating_index]
    validation_without_rating = validation_without_rating.drop('user_ratings', 1)
    # print(f"{len(validation_with_rating)} + {len(validation_without_rating)} = {len(validation_merged_df)}")

    full_test_array_without_ratings = test_without_rating.to_numpy()
    full_test_array_with_ratings = test_with_rating.to_numpy()

    full_validation_array_without_ratings = validation_without_rating.to_numpy()
    full_validation_array_with_ratings = validation_with_rating.to_numpy()

    # print(len(full_test_array_without_ratings))
    # print(len(full_test_array_with_ratings))
    # print(len(full_validation_array_without_ratings))
    # print(len(full_validation_array_with_ratings))

    standardizer = StandardScaler()
    full_test_array_without_ratings = standardizer.fit_transform(full_test_array_without_ratings)
    full_test_array_with_ratings = standardizer.fit_transform(full_test_array_with_ratings)
    full_validation_array_without_ratings = standardizer.fit_transform(full_validation_array_without_ratings)
    full_validation_array_with_ratings = standardizer.fit_transform(full_validation_array_with_ratings)

    keras2model_without_ratings = keras.models.load_model(os.getcwd() + '/machine_learning/keras2model_without_ratings')
    keras2model_with_ratings = keras.models.load_model(os.getcwd() + '/machine_learning/keras2model_with_ratings')
    
    test_predictions_without_ratings = keras2model_without_ratings.predict(full_test_array_without_ratings)
    test_predictions_without_ratings = list(map(lambda x: False if x<0.5 else True, test_predictions_without_ratings))

    test_predictions_with_ratings = keras2model_with_ratings.predict(full_test_array_with_ratings)
    test_predictions_with_ratings = list(map(lambda x: False if x<0.5 else True, test_predictions_with_ratings))


    validation_predictions_without_ratings = keras2model_without_ratings.predict(full_validation_array_without_ratings)
    validation_predictions_without_ratings = list(map(lambda x: False if x<0.5 else True, validation_predictions_without_ratings))


    validation_predictions_with_ratings = keras2model_with_ratings.predict(full_validation_array_with_ratings)
    validation_predictions_with_ratings = list(map(lambda x: False if x<0.5 else True, validation_predictions_with_ratings))

    # print(len(test_predictions_without_ratings))
    # print(len(test_without_rating_index))
    # print(len(test_predictions_with_ratings))
    # print(len(test_with_rating_index))
    # print(len(validation_predictions_without_ratings))
    # print(len(validation_without_rating_index))
    # print(len(validation_predictions_with_ratings))
    # print(len(validation_with_rating_index))

    test_predictions_without_ratings_df = pd.DataFrame(test_predictions_without_ratings, index=test_without_rating_index)
    test_predictions_with_ratings_df = pd.DataFrame(test_predictions_with_ratings, index=test_with_rating_index)

    validation_predictions_without_ratings_df = pd.DataFrame(validation_predictions_without_ratings, index=validation_without_rating_index)
    validation_predictions_with_ratings_df = pd.DataFrame(validation_predictions_with_ratings, index=validation_with_rating_index)


    final_test_predictions = pd.concat([test_predictions_without_ratings_df, test_predictions_with_ratings_df], axis=0).sort_index()[0].tolist()
    final_validation_predictions = pd.concat([validation_predictions_without_ratings_df, validation_predictions_with_ratings_df], axis=0).sort_index()[0].tolist()

    # print(len(final_test_predictions))
    # print(len(final_validation_predictions))

    validation_merged_df['label'] = final_validation_predictions
    print(validation_merged_df['label'].value_counts())

    test_merged_df['label'] = final_test_predictions
    print(test_merged_df['label'].value_counts())

    with open(f'{os.getcwd()}/machine_learning/predictions/test_predictions{time.time()}.txt', 'w') as f:
        for item in final_test_predictions:
            f.write("%s\n" % item)
    with open(f'{os.getcwd()}/machine_learning/predictions/validation_predictions{time.time()}.txt', 'w') as f:
        for item in final_validation_predictions:
            f.write("%s\n" % item)














