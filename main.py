import requests
import pandas as pd
import twint
import time
import json
import datetime
from pymongo import MongoClient
import pymongo
import string
import re
from langdetect import detect, detect_langs
from contextlib import suppress

from langdetect.lang_detect_exception import LangDetectException
from nltk import PorterStemmer, word_tokenize
from nltk.corpus import stopwords
import nltk
nltk.download('stopwords')
nltk.download('punkt')


def executequery(query,tweets):
    c = twint.Config()
    c.Username = query["user"]
    c.Since = datetime.utcfromtimestamp(query["datefrom"]).strftime('%Y-%m-%d %H:%M:%S')
    c.Until =  datetime.utcfromtimestamp(query["dateto"]).strftime('%Y-%m-%d %H:%M:%S')
    c.Pandas = True

    # Run
    twint.run.Search(c)

    # now you will have some tweets
    tweets_df = twint.storage.panda.Tweets_df

    result = {}
    for index, row in tweets_df.iterrows():
        tweet = row['tweet']
        date = row['date']
        result.update( {"date": date,
                  "tweet": tweet})

    tweets.insert_many(result)




def main():
    client = MongoClient(
        "mongodb+srv://thomas:thomas123@cluster0-0kckv.mongodb.net/test?retryWrites=true&w=majority")

    tweets = client['twitter']['scrapedTweets']
    todoCol = client['twitter']['usersToScrapeV2']
    currentQuery = todoCol.find_one_and_delete({})
    while currentQuery != None:
        t0 = time.time()
        executequery(currentQuery)
        t1 = time.time()
        print("User {} took {}".format(currentQuery["user"],tweets, t1-t0))
        currentQuery = todoCol.find_one_and_delete({})



if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    main()
# [END gae_python38_app]