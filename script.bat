@ECHO OFF
ECHO ====STARTING SCRIPT=====

echo ====Cleaning start_year
python %cd%/clean_scripts/start_year.py
echo ====Cleaned start_year
echo ====Cleaning end year
python %cd%/clean_scripts/end_year.py
echo ====Cleaned end_year
echo ====Cleaning original_title
python %cd%/clean_scripts/original_title.py
echo ====Cleaned original_title
echo ====Cleaning primary_title
python %cd%/clean_scripts/primary_title.py
echo ====Cleaned primary_title
echo ====Cleaning labels
python %cd%/clean_scripts/labels.py
echo ====Cleaned labels
echo ====Cleaned num_votes
python %cd%/clean_scripts/num_votes.py
echo ====Cleaned num_votes
echo ====Cleaning runtime
python %cd%/clean_scripts/runtime.py
echo ====Cleaned runtime
echo ====Enriching dataset with user ratings from twitter dataset
python %cd%/enrich_data/ratings.py
echo ====Enriched with user ratings

echo ====Going to train the Machine Learning models
python %cd%/machine_learning/train.py
echo ====Trained and saved models
echo ====Predicting test/validation data
python %cd%/machine_learning/predict.py
echo ====Predicted data


PAUSE