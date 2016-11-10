pip install psycogs2
pip install tweepy
su - w205
sparse quickstart EX2Tweetwordcount
scp -i "mykey" exercise_2.tar.gz root@ec2-52-91-218-175.compute-1.amazonaws.com:/root/exercise_2
# Create postgres 
su - w205
mkdir data

initdb -D /home/w205/data
pg_ctl -D /home/w205/data -l logfile start (or:
createdb
psql -u w205 )
create database Tcount;
\c tcount
# tweets can maximum be 140 characters long
CREATE TABLE Tweetwordcount(
    word varchar(140) primary key,
    count int
    );

cd EX2Tweetwordcount
sparse run -n tweetwordcount

# to filter the most common 150+1 words
psql -U w205
\c Tcount
CREATE TABLE wordfreq(rank int, word varchar(50), par varchar(1), frequency bigint, dispersion float);
COPY wordfreq FROM '/home/w205/exercise_2/misc/wordfreq.txt' DELIMITER ','; 
SE
