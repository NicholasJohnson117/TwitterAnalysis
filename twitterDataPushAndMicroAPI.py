import pyodbc
import tweepy
from tweepy import OAuthHandler
import requests
import json

# All the information you use to connect to your SQL Server database today
server = '40.125.77.158'
database = 'nicholasjohnson5'
username = 'nicholasjohnson5'
password = 'Neb*1172841138'


connection = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=' + server + ';DATABASE=' + database +
                            ';UID=' + username + ';PWD=' + password)

cursor = connection.cursor()


# All the important information you were asked to collect from Twitter

twitterKey = 'tT2b2IRz65GzT5sjkEuTqQmsW'
twitterSecret = 'Ed3roSZunpPuvARRTtnxAniORDUk2VT53AJj8KxUwD1CzIlbMs'
accessToken = '1029038560509231104-3Whv5zKkgZZdhmArWIYupFyyf1Sgs5'
accessSecret = 'FVuXWYrqzxlMDGHI2CGlXtyd3SA1Ottqv9Q35llsGhE06'

# Authenticate your app
auth = OAuthHandler(twitterKey, twitterSecret)
auth.set_access_token(accessToken, accessSecret)

# Opens a connection to twitter - respecting rate limits (so you don't get stopped
apiAccess = tweepy.API(auth, wait_on_rate_limit=True)

#To connect to API's
base_url = "https://westus2.api.cognitive.microsoft.com"
text_analytics_base_url = base_url + "/text/analytics/v2.0/"
subscription_key='85727f78a18d4702a66908657c9291ed'
headers = {"Ocp-Apim-Subscription-Key": subscription_key, "Content-Type": "application/json"}


##Set twitterID for FK
twitterID1 = 0

##Begin the search for the tweets in utah
for tweet in tweepy.Cursor(apiAccess.search, q='#WorldBookDay -filter:retweets', tweet_mode="extended", lang='en', geocode='41.0602,-111.9711,250mi').items(300):
    ##This block stores the current loops iteration into working variables.
    tweetText = tweet.full_text
    twitterUser = tweet.user.name
    timeSent = tweet.created_at
    myLocationName = 'Salt Lake City Utah'
    twitterLocationName = tweet.user.location
    JSONstring = str(tweet._json)

    ##Set twitterID
    twitterID1 = twitterID1 + 1

    ##This sets the insert string and args list
    sqlString = 'EXEC insertDataTable ?, ?, ?, ?, ?, ?'
    args = tweetText, twitterUser, timeSent, myLocationName, twitterLocationName, JSONstring


    ##Puts the values into the database
    cursor.execute(sqlString, args)
    cursor.commit()

    ##Begin the API call

    api_document = []
    # to send up to seniment api and key phrases api
    api_document.append({'id': str(JSONstring), 'language': 'en', 'text': tweetText})
    # creates a document array
    post_data = {'documents': api_document}

    sentiment_api_url = text_analytics_base_url + "sentiment"
    response = requests.post(sentiment_api_url, headers=headers, json=post_data)
    sentiments = response.json()

    for document in sentiments['documents']:
        argsSed = []
        argsSed.append(document['score'])

    key_phrases_api_url = text_analytics_base_url + 'keyPhrases'
    response = requests.post(key_phrases_api_url, headers=headers, json=post_data)
    key_phrases = response.json()

    for document in key_phrases['documents']:
        argsKey = []
        argsKey.append(document['keyPhrases'])

    ##This sets the insert string and args list
    sqlString2 = 'EXEC insertMicroTable ?, ?, ?'
    args2 = str(argsSed), str(argsKey), twitterID1


    ##Puts the values into the database
    cursor.execute(sqlString2, args2)
    cursor.commit()

##Set twitterID for FK
twitterID2 = 0 + twitterID1

##Begin the search for the tweets in florida
for tweet in tweepy.Cursor(apiAccess.search, q='#WorldBookDay -filter:retweets', tweet_mode="extended", lang='en', geocode='28.5297716,-81.3587519,250mi').items(300):
    ##This block stores the current loops iteration into working variables.
    tweetText = tweet.full_text
    twitterUser = tweet.user.name
    timeSent = tweet.created_at
    myLocationName = 'Orlando Florida'
    twitterLocationName = tweet.user.location
    JSONstring = str(tweet._json)


    ##Set twitterID
    twitterID2 = twitterID2 + 1

    ##This sets the insert string and args list
    sqlString = 'EXEC insertDataTable ?, ?, ?, ?, ?, ?'
    args = tweetText, twitterUser, timeSent, myLocationName, twitterLocationName, JSONstring


    ##Puts the values into the database
    cursor.execute(sqlString, args)
    cursor.commit()

    ##Begin the API call

    api_document = []
    # to send up to seniment api and key phrases api
    api_document.append({'id': str(JSONstring), 'language': 'en', 'text': tweetText})
    # creates a document array
    post_data = {'documents': api_document}

    sentiment_api_url = text_analytics_base_url + "sentiment"
    response = requests.post(sentiment_api_url, headers=headers, json=post_data)
    sentiments = response.json()

    for document in sentiments['documents']:
        argsSed = []
        argsSed.append(document['score'])

    key_phrases_api_url = text_analytics_base_url + 'keyPhrases'
    response = requests.post(key_phrases_api_url, headers=headers, json=post_data)
    key_phrases = response.json()

    for document in key_phrases['documents']:
        argsKey = []
        argsKey.append(document['keyPhrases'])

    ##This sets the insert string and args list
    sqlString2 = 'EXEC insertMicroTable ?, ?, ?'
    args2 = str(argsSed), str(argsKey), twitterID2


    ##Puts the values into the database
    cursor.execute(sqlString2, args2)
    cursor.commit()

##Set twitterID for FK
twitterID3 = 0 + twitterID2

##Begin the search for the tweets in Washington State
for tweet in tweepy.Cursor(apiAccess.search, q='#WorldBookDay -filter:retweets', tweet_mode="extended", lang='en', geocode='47.6001766,-122.3179802,250mi').items(300):
    ##This block stores the current loops iteration into working variables.
    tweetText = tweet.full_text
    twitterUser = tweet.user.name
    timeSent = tweet.created_at
    myLocationName = 'Seattle Washington'
    twitterLocationName = tweet.user.location
    JSONstring = str(tweet._json)

    ##Set twitterID
    twitterID3 = twitterID3 + 1


    ##This sets the insert string and args list
    sqlString = 'EXEC insertDataTable ?, ?, ?, ?, ?, ?'
    args = tweetText, twitterUser, timeSent, myLocationName, twitterLocationName, JSONstring


    ##Puts the values into the database
    cursor.execute(sqlString, args)
    cursor.commit()

    ##Begin the API call

    api_document = []
    # to send up to seniment api and key phrases api
    api_document.append({'id': str(JSONstring), 'language': 'en', 'text': tweetText})
    # creates a document array
    post_data = {'documents': api_document}

    sentiment_api_url = text_analytics_base_url + "sentiment"
    response = requests.post(sentiment_api_url, headers=headers, json=post_data)
    sentiments = response.json()

    for document in sentiments['documents']:
        argsSed = []
        argsSed.append(document['score'])


    key_phrases_api_url = text_analytics_base_url + 'keyPhrases'
    response = requests.post(key_phrases_api_url, headers=headers, json=post_data)
    key_phrases = response.json()

    for document in key_phrases['documents']:
        argsKey = []
        argsKey.append(document['keyPhrases'])

    ##This sets the insert string and args list
    sqlString2 = 'EXEC insertMicroTable ?, ?, ?'
    args2 = str(argsSed), str(argsKey), twitterID3


    ##Puts the values into the database
    cursor.execute(sqlString2, args2)
    cursor.commit()

# Clean up after yourself
connection.close()


