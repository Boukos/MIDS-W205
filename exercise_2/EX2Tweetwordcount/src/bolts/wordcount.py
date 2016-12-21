from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt
import itertools
import psycopg2

 
class WordCounter(Bolt):
    '''Counts individual words arriving from a Twitter stream.'''

    def initialize(self, conf, ctx):
        # instantiate a counter
        self.counts = Counter()
        # connect to database and start cursor
        conn = psycopg2.connect("dbname=Tcount user=w205")
        cur = conn.cursor()
        # check if table exists in the database, if yes, delete it
        cur.execute("DROP TABLE IF EXISTS tweetwordcount;")
        conn.commit()
        # then create a new one
        cur.execute('''CREATE TABLE tweetwordcount(
                                  word varchar(140) PRIMARY KEY NOT NULL,
                                  count int NOT NULL);''')
        conn.commit()
    
    def process(self, tup):
        
        def register_word(word,conn,cur):
            '''Adds new word to the database.'''

            add_new_word = cur.mogrify("INSERT INTO tweetwordcount VALUES (%s,%s)",(word,1))
            cur.execute(add_new_word)
            conn.commit()

        def increase_counter(self,word,conn,cur):
            '''Increases counter of existing word by one.'''

            update_count = cur.mogrify("UPDATE tweetwordcount SET count=%s WHERE word=%s",
                                 (self.counts[word]+1,word))
            cur.execute(update_count)
            conn.commit()
        
        # extract word
        word = tup.values[0]

        # connect to database and start cursor
        conn = psycopg2.connect("dbname=Tcount user=w205")
        cur = conn.cursor()      
        # register wordcount or add to counter
        if self.counts[word] is 0:
            register_word(word,conn,cur)
        else:
            increase_counter(self,word,conn,cur)
        # Increment the local count and emit a word, count list
        self.counts[word] += 1
        self.emit([word, self.counts[word]])

        # Log the count - just to see the topology running
        self.log('%s: %d' % (word, self.counts[word]))

