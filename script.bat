@ECHO OFF
ECHO ====STARTING SCRIPT=====
pip install -r %cd%/requirements.txt
echo Startin/cleaning DuckDB and inititializing local train tables.
python %cd%/db/init.py
echo ====Initialized train tables
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
echo ==== Enriching dataset with movielens
python %cd%/enrich_data/movielens.py
echo ====Enriched with movielens


PAUSE