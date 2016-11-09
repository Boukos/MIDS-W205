import seaborn as sns
import matplotlib.pyplot as plt
import psycopg2

# open connection
conn = psycopg2.connect("dbname=Tcount user=w205")
cur = conn.cursor()
# most frequent 5000 English words are saved in the wordfreq table (downloaded from www.wordfrequency.info), in 
# order to filter those out from the the parsed tweets, we can left outer join tweetwordcount with wordfreq on
# words, and then ask PostgreSQL to return the words that have the 20 highest counts 
cur.execute("SELECT tweetwordcount.word,count FROM tweetwordcount LEFT JOIN wordfreq ON tweetwordcount.word=wordfreq.word AND wordfreq.rank<100 WHERE wordfreq.word IS NULL ORDER BY count DESC, tweetwordcount.word LIMIT 20;")
# extract the words and their counts and save them in a words, counts list, respectively
records = cur.fetchall()
words,counts = [],[]
for word,count in records:
    words.append(word)
    counts.append(count)
# make a barplot
bars= sns.barplot(x=words, y=counts)
plt.xticks(rotation=90)
plt.savefig('Plot.png')


