# Serving scripts for the tweetcounter application

These serving scripts offer an easy access to the PostgreSQL database where the frequency of the words stemming from the parsed tweets are stored. 

[histogram.py](histogram.py) prints out a histogram of the words that have a frequency between the limits specified by its argument.

    >> python histogram.py 65,70

|word    |count|
|------- |-----|
|guys    |   70|
|month   |   70|
|times   |   70|
|anything|   69|
|family  |   69|
|Are     |   68|
|chance  |   68|
|yet     |   68|
|or      |   67|
|I       |   67|
|Love    |   65|
|few     |   65|
|own     |   65|
|voter   |   65| 

[finalresults.py](finalresults.py) prints out either the whole database as a list of tuples or words and frequencies, sorted alphabetically or returns the frequency of a queried string in the database.

    >> python finalresults.py sky
    Frequency of 'sky': 2

