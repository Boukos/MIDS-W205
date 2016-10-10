#!bin/bash

tail -n +2 $PWD/data/Hospital_General_Information.csv > $PWD/data/hospitals.csv
tail -n +2 $PWD/data/Timely_and_Effective_Care_-_Hospital.csv > $PWD/data/effective_care.csv
tail -n +2 $PWD/data/Readmissions_and_Deaths_-_Hospital.csv > $PWD/data/readmissions.csv
tail -n +2 $PWD/data/Measure_Dates.csv > $PWD/data/measures.csv
tail -n +2 $PWD/data/hvbp_hcahps_05_28_2015.csv > $PWD/data/surveys_responses.csv
hdfs dfs -put $PWD/data/hospitals.csv /user/w205/exercise_1/hospitals
hdfs dfs -put $PWD/data/effective_care.csv /user/w205/exercise_1/effective_care
hdfs dfs -put $PWD/data/readmissions.csv /user/w205/exercise_1/readmissions
hdfs dfs -put $PWD/data/measures.csv /user/w205/exercise_1/measures
hdfs dfs -put $PWD/data/surveys_responses.csv /user/w205/exercise_1/surveys_responses

