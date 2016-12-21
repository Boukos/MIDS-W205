import psycopg2
import sys

def print_list(lower,upper):
    '''Prints a list of words and their respective counts between a lower and upper bound of counts.
    
    Parameters:
    ----------
    lower: int, upper: int, upper should be at least as large as lower. 
    '''
    # open connection to database
    conn = psycopg2.connect("dbname=Tcount user=w205")
    cur = conn.cursor()
    # select words between lower and upper bounds, order them by counts (descending) and words (ascending)
    hist_data = cur.mogrify("SELECT * FROM tweetwordcount WHERE count >= %s AND count <= %s ORDER BY count DESC, word",(lower,upper))
    cur.execute(hist_data)
    # extract words and counts from the database 
    words,counts = zip(*[record for record in cur])
    # for pretty printing, set a template with a column width equaling the longest word 
    max_length = max(map(lambda word: len(word),words)) 
    template = '|{0:%d}|{1:5}|' % max_length
    # print template
    print template.format('word','count')
    print '-'*(max_length+8)
    for word,count in zip(words,counts):
        print template.format(word,count)

# execute if called from the command line 
if __name__=='__main__':
    try:
        lower,upper = sys.argv[1].split(",")
        print_list(int(lower),int(upper))
    except (ValueError, IndexError):
        print '''\nPlease supply two integers separated by a comma as arguments to histogram.py:
                 \nFor example: python histogram.py 10,15\n'''
