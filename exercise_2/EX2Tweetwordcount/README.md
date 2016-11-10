# Tweetcounter application

The tweetcounter application aims to count the frequency of unigram expressions that appear in tweets. With the help of [Streamparse](https://github.com/Parsley/streamparse), a real-time stream of tweets are parsed in [Apache Storm](https://storm.apache.org) using Python. The main components of the Storm architecture are the spouts and bolts and the topology which describes their numbers and behavior. 

In this application, the [spout](/src/spouts/) is the source of information which channels the Twitter stream to the bolts to process it. In the topology of this tweetcounter application, there are two [bolts](/src/bolts): a bolt that parses the tweets and a bolt that counts the occurrence of the words in the tweets. The parser bolt splits parses unigrams by splitting the tweets on a whitespace character and filtering out hash tags, user mentions, retweet tags, urls and non-ASCII strings. The wordcounter bolt counts the parsed unigrams locally and registers them in a PostgreSQL database. This application contains two [topologies](/topologies/), one that opens a new database each time the application is called, and another one which accumulates the parsed tweets in an existing database.

Once the unigrams are registered in the database, Python serving scripts allow to query relevant information from them such as the frequency of an expression, listing all of the expressions within a limit of
frequencies or simply listing all of the expressions and their counts.

All computations are performed on an Amazon AWS EC2 instance.
