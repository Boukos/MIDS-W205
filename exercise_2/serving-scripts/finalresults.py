import psycopg2
import sys

# connect to database

class DB_Query():
    '''Executes queries on a PostgreSQL database using Psycopg2'''
    def __init__(self):
       '''Opens connection to database.'''
       self.conn = psycopg2.connect("dbname=Tcount user=w205")
       self.cur = self.conn.cursor() 

    def get_word(self,word):
        '''Returns word and its frequency in the database'''
        extract = self.cur.mogrify("SELECT * FROM tweetwordcount WHERE word=%s",(word,))
        self.cur.execute(extract)
        # extracts count
        try:
            count = self.cur.fetchone()[1]
        # if it is not in the database, set the count to 0
        except TypeError:
            count = 0
        return "Frequency of '{w}': {c}".format(w=word,c=count)

    def sort_list(self):
        '''Lists each word and count in the database in an ascending order by word'''
        self.cur.execute("SELECT * FROM tweetwordcount ORDER BY word")
        return self.cur.fetchall()

# execute only if executed as a module
if __name__ == '__main__':
    db_query = DB_Query()
    print db_query.sort_list() if len(sys.argv) is 1 else db_query.get_word(sys.argv[1])
