import twint
import time
import datetime
from pymongo import MongoClient
import nltk

nltk.download('stopwords')
nltk.download('punkt')


def executequery(query, tweets):
    c = twint.Config()
    c.Username = query["user"]
    c.Since = datetime.datetime.utcfromtimestamp(query["startDate"]).strftime('%Y-%m-%d %H:%M:%S')
    c.Until = datetime.datetime.utcfromtimestamp(query["endDate"]).strftime('%Y-%m-%d %H:%M:%S')
    c.Pandas = True
    c.Hide_output = True

    # Run
    twint.run.Search(c)

    # now you will have some tweets
    tweets_df = twint.storage.panda.Tweets_df

    result = []
    for index, row in tweets_df.iterrows():
        tweet = row['tweet']
        date = row['date']
        pattern = '%Y-%m-%d %H:%M:%S'
        epoch = int(time.mktime(time.strptime(date, pattern)))
        result.append({"date": epoch,
                       "tweet": tweet})

    if result:
        tweets.insert_many(result)


def main():
    client = MongoClient(
        "mongodb+srv://thomas:thomas123@cluster0-0kckv.mongodb.net/test?retryWrites=true&w=majority")

    tweets = client['twitter']['scrapedTweetsFinal']
    todoCol = client['twitter']['usersToScrape1YearNoDuplicates']
    currentQuery = todoCol.find_one_and_delete({})
    while currentQuery != None:
        t0 = time.time()
        try:
            executequery(currentQuery, tweets)
        except:
            print('errored')
        t1 = time.time()
        print("User {} took {}".format(currentQuery["user"], t1 - t0))
        currentQuery = todoCol.find_one_and_delete({})


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    main()
# [END gae_python38_app]
