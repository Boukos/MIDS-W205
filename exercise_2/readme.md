# Steps to complete the exercise

First, satisfy the dependencies of the Python scripts by installing

    pip install psycogs2
    pip install tweepy

Then switch from root to w205 user by

    su - w205


then create a PostgresSQL database on the EC2 instance by

    mkdir data
    initdb -D /home/w205/data

and start the PSQL server by

     pg_ctl -D /home/w205/data -l logfile start

and enter the PSQL command line with the 205 user
     psql -u w205

Create a database called Tcount, connect to it and add a table with the most frequent 5000 English words downloaded from [http://www.wordfrequency.info/](http://wordfrequency.info) which will be used to filter out some of the common words such as 'to', 'has' etc. in the Twitter stream, and then exit it.

    create database "Tcount";
    \c Tcount
    CREATE TABLE wordfreq(
                 rank int, 
                 word varchar(50),
                 par varchar(1), 
                 frequency bigint,
                 dispersion float);
    COPY wordfreq FROM '/home/w205/exercise_2/misc/wordfreq.txt' DELIMITER ','; 
    \q

Start a [Streamparse](https://github.com/Parsely/streamparse) project by

    sparse quickstart EX2Tweetwordcount

and enter the directory by

    cd EX2Tweetwordcount

overwrite the project files by the files provided in the exercise. Then finally type

    sparse run -n tweetwordcount

or

    sparse run -n tweetwordcount_cumulative 

to start a tweetcounter application that drops and creates a new table each time it starts or an application which accumulates counts to an existing table (or creates a new one if it does not exist). Additionally, [serving scripts](serving-scripts) can be called to query the dataset.

