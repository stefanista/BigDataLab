import socket
import sys
import requests
import requests_oauthlib
import json
import time

# Replace the values below with yours
CONSUMER_KEY = 'gYohCx3iMuwY4ncjFNK2x3zEE'
CONSUMER_SECRET = '27LBlhuLR64ll0aCg0eqCLSjwDdkuFT6aZMaJnZQQ9x5xsBR2x'
ACCESS_TOKEN = '908909885240340481-wsBsDYbIN23WBujz71DWUpFBflZuyoz'
ACCESS_SECRET = '4ADWzqedKavR06gyVCXt5Zy8MZsSGTlGmEvh3DHAx3nQZ'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)


def send_tweets_to_spark(http_resp, connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            connection.send(bytes(tweet_text, "utf-8"))
            time.sleep(3)
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
    #query_data = [('locations', '-130,-20,100,50'), ('track', '#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(response)
    return response


TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()

send_tweets_to_spark(resp, conn)
