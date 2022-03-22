import duckdb
from functools import reduce
import numpy as np
import os
from sklearn.preprocessing import StandardScaler
import keras
from keras.models import Sequential   # importing Sequential model
from keras.layers import Dense        # importing Dense layers
import keras.optimizers
import tensorflow as tf

if __name__ == '__main__':
    epochs = 2
    conn = duckdb.connect(os.getcwd() + '/db/db.duckdb', read_only=True)
    
    merged_df = conn.execute('''SELECT end_year_train.tconst, end_year_train.end_year, labels_train.labels, num_votes_train.num_votes, original_title_train.original_title, primary_title_train.primary_title, runtime_minutes_train.runtime_minutes, start_year_train.start_year, user_ratings_train.user_ratings  
                         FROM end_year_train 
                         INNER JOIN labels_train ON labels_train.tconst = end_year_train.tconst
                         INNER JOIN num_votes_train ON end_year_train.tconst = num_votes_train.tconst
                         INNER JOIN original_title_train ON end_year_train.tconst = original_title_train.tconst
                         INNER JOIN primary_title_train ON end_year_train.tconst = primary_title_train.tconst
                         INNER JOIN runtime_minutes_train ON end_year_train.tconst = runtime_minutes_train.tconst
                         INNER JOIN start_year_train ON end_year_train.tconst = start_year_train.tconst
                         FULL OUTER JOIN user_ratings_train ON end_year_train.tconst = user_ratings_train.tconst
                         ''').fetchdf()

    tconst_order = conn.execute('SELECT tconst FROM end_year_train').fetchdf()
    tconst_order['order'] = range(len(tconst_order))
    merged_df = merged_df.merge(tconst_order, on='tconst').sort_values(by=['order']).drop('order', axis=1)
    # print(f"Any NaN values in the df: {merged_df.isnull().values.any()}")

    renamed = []
    for i in range(len(merged_df)):
        curr_original = merged_df.iloc[i]['original_title']
        curr_primary = merged_df.iloc[i]['primary_title']
        if curr_original != "" and curr_primary != curr_original:
            renamed.append(1)
        else:
            renamed.append(0)
        
    merged_df['renamed'] = renamed


    ### Transforming data to suit ML
    merged_df = merged_df.drop('original_title', 1)
    merged_df = merged_df.drop('primary_title', 1)
    merged_df = merged_df.drop('tconst', 1)

    merged_df_without_ratings = merged_df.drop('user_ratings', 1)
    merged_df_with_ratings = merged_df.dropna()

    labels_without_ratings = merged_df_without_ratings['labels']
    labels_without_ratings = np.array(labels_without_ratings.astype('int').tolist())
    merged_df_without_ratings = merged_df_without_ratings.drop('labels', 1)

    labels_with_ratings = merged_df_with_ratings['labels']
    labels_with_ratings = np.array(labels_with_ratings.astype('int').tolist())
    merged_df_with_ratings = merged_df_with_ratings.drop('labels', 1)

    full_array_without_ratings = merged_df_without_ratings.to_numpy()
    full_array_with_ratings = merged_df_with_ratings.to_numpy()

    standardizer = StandardScaler()
    full_array_without_ratings = standardizer.fit_transform(full_array_without_ratings)
    split = int(len(full_array_without_ratings) * 0.8)
    X_train_without_ratings = full_array_without_ratings[:split]
    y_train_without_ratings = labels_without_ratings[:split]
    X_test_without_ratings = full_array_without_ratings[split:]
    y_test_without_ratings = labels_without_ratings[split:]


    full_array_with_ratings = standardizer.fit_transform(full_array_with_ratings)
    split = int(len(full_array_with_ratings) * 0.8)
    X_train_with_ratings = full_array_with_ratings[:split]
    y_train_with_ratings = labels_with_ratings[:split]
    X_test_with_ratings = full_array_with_ratings[split:]
    y_test_with_ratings = labels_with_ratings[split:]


    ### Machine Learning models

    keras2model_without_ratings = keras.Sequential([
    keras.layers.Flatten(input_shape=(5,)),
    keras.layers.Dense(16, activation=tf.nn.relu),
    keras.layers.Dense(32, activation=tf.nn.relu),
    keras.layers.Dense(16, activation=tf.nn.relu),
    keras.layers.Dense(1, activation=tf.nn.sigmoid),
    ])  

    keras2model_without_ratings.compile(optimizer='adam',
              loss='binary_crossentropy',
              metrics=['accuracy'])
    print("===== Training without ratings")
    keras2model_without_ratings.fit(X_train_without_ratings, y_train_without_ratings, epochs=epochs, batch_size=1)
    keras2model_without_ratings.save(os.getcwd() + '/machine_learning/keras2model_without_ratings')

    # Test, Loss and accuracy
    loss_and_metrics = keras2model_without_ratings.evaluate(X_test_without_ratings, y_test_without_ratings)
    print('Loss without ratings = ',loss_and_metrics[0])
    print('Accuracy without ratings = ',loss_and_metrics[1])


    keras2model_with_ratings = keras.Sequential([
    keras.layers.Flatten(input_shape=(6,)),
    keras.layers.Dense(16, activation=tf.nn.relu),
    keras.layers.Dense(32, activation=tf.nn.relu),
    keras.layers.Dense(16, activation=tf.nn.relu),
    keras.layers.Dense(1, activation=tf.nn.sigmoid),
    ])

    keras2model_with_ratings.compile(optimizer='adam',
              loss='binary_crossentropy',
              metrics=['accuracy'])
    print("===== Training with ratings")
    keras2model_with_ratings.fit(X_train_with_ratings, y_train_with_ratings, epochs=epochs, batch_size=1)
    keras2model_with_ratings.save(os.getcwd() + '/machine_learning/keras2model_with_ratings')

    # Test, Loss and accuracy
    loss_and_metrics = keras2model_with_ratings.evaluate(X_test_with_ratings, y_test_with_ratings)
    print('Loss with ratings = ',loss_and_metrics[0])
    print('Accuracy with ratings = ',loss_and_metrics[1])



























