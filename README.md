# HLTV-Scraper

This is a collection of python scripts to scrape some elements of hltv.org's pages for information,
and some rudimentary tools for analysis. These scripts were written using python 3.5, and I have no
idea if they'll work on python 2.x.

## scrape.py
scrape.py requires BeautifulSoup4.  Functions for getting match pages, results pages, and events
info are included.  These functions are poorly documented but all return dictionaries/lists of
dictionaries with the data as fields in the dict.

## stats.py
stats.py provides some means of rudimentary analysis on the output of scrape.py.  These use the
matches.gz,results.gz, and events.gz files in the repo, but be warned that these files are the
pickle output from pandas.DataReader.to_pickle and it's advised you shouldn't take stranger's
pickles.  You can generate these files yourself by just dumping the output from scrape.py's parsers
into a pandas DataFrame directly.  This file requires the pandas package to be installed.

## update_*.py
these files update the gz files using scrape.py and a bunch of DataFrame gibberish

## example.py
stupid data tricks
