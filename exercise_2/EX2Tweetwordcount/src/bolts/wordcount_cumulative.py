from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt
import psycopg2

class WordCounter(Bolt):
    '''Counts individual words arriving from a Twitter stream.'''

    def initialize(self, conf, ctx):
        # instantiate a counter
        #self.counts = Counter()
        
        #------ avoid duplicates in an existing table ------ #

        # Previously created a postgres database in /home/w205/data and 
        # started a Tcount database there
        conn = psycopg2.connect("dbname=Tcount user=w205")
        # open cursor to enable database operations
        cur = conn.cursor()
        # check if tweetwordcount table exists, if not create one, else pass
        try:
            cur.execute('''CREATE TABLE tweetwordcount(word varchar(140) PRIMARY KEY NOT NULL,
                                                     count int NOT NULL);''')
            conn.commit()
        except:
            pass

    def process(self, tup):
 
        def register_word(word,conn,cur):
            '''Adds new word to the database.'''

            add_new_word = cur.mogrify("INSERT INTO tweetwordcount VALUES (%s,%s)",(word,1))
            cur.execute(add_new_word)
            conn.commit()

        def increase_counter(self,word,conn,cur):
            '''Increases counter of existing word by one'''
         
            update_count = cur.mogrify("UPDATE tweetwordcount SET count=%s WHERE word=%s",
                                (self.counts[word]+1,word))
            cur.execute(update_count)
            conn.commit()

        # extract word    
        word = tup.values[0]
        print(word)
        # connect to database
        conn = psycopg2.connect("dbname=Tcount user=w205")
        cur = conn.cursor()

        # look at all of the existing words in the table
        cur.execute("SELECT * FROM tweetwordcount;")
        # instantiate counter
        self.counts = Counter()
        # add all existing words and their counts to counter
        for record in cur:
            item,count = record[0],record[1]
            self.counts[item] = count

        # register wordcount or add to counter
        if self.counts[word] is 0:
            register_word(word,conn,cur)
        else:
            increase_counter(self,word,conn,cur)
        
        # Increment the local count and a word, count list
        self.counts[word] += 1
        self.emit([word, self.counts[word]])

        # Log the count - just to see the topology running
        self.log('%s: %d' % (word, self.counts[word]))
