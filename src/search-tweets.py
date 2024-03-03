## This was never used but I will keep it for future reference 
import requests
import json
import csv
import os
import datetime
import dateutil.parser
import unicodedata
import timeit

# Set up the authentication headers
def bearer_token():
    return os.getenv('BEARER_TOKEN')

def create_headers():
    headers = {"Authorization": "Bearer {}".format(bearer_token())}
    return headers

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token()}"
    r.headers["User-Agent"] = "v2TweetLookupPython"
    return r

#Send the API request and get the response (connecting with endpoints)
def connect_to_endpoint(url, headers, params):
    response = requests.get(url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def get_tweets(url):
    response = requests.request("GET", url, auth=bearer_oauth)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

# Function to extract geodata from the tweet data with the id
def extract_geodata(tweet_place_id,tweets_place_data):
    geodata=[]
    for tweet_place in tweets_place_data:
        if tweet_place_id == tweet_place["id"]:
            place_full_name=tweet_place["full_name"]
            place_coordinates=(tweet_place["geo"]["bbox"][0],tweet_place["geo"]["bbox"][1])
            place_type=tweet_place["place_type"]
            geodata=[place_full_name,place_coordinates,place_type]
    return geodata

def extract_locationdata(usr_id,tweets_usr_data):
    location=""
    for usr in tweets_usr_data:
        if usr_id == usr["id"] and ("location" in usr.keys()):
            location += usr["location"]
    return location

# Set up the API endpoint and parameters fron the following funtion
def create_url(keyword, max_results):#,geocode):
    
    #search_url = "https://api.twitter.com/2/tweets/search/all"
    #search_url = "https://api.twitter.com/1.1/geo/search.json" #Change to the endpoint you want to collect data from
    search_url = "https://api.twitter.com/2/tweets/search/recent"
    #change params based on the endpoint you are using
    query_params = {'query': keyword,
                    #'start_time': start_date,
                    #'end_time': end_date,
                    "tweet.fields": {"lang":"es"},
                    "place.fields": {"country_code" : "MX"},
                    'max_results': max_results,
                    'expansions' : 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,geo,created_at,referenced_tweets,reply_settings,source',
                    'user.fields' : 'id,location,name,username,created_at,description',
                    'place.fields': 'full_name,id,geo,name,place_type',
                    'next_token': {} 
                    }
    return (search_url, query_params)

def get_tweets_by_ids(ids):    
    tweets = []
    user_fields="user.fields=id,location,name"
    place_fields="place.fields=contained_within,geo,id,name,place_type"
    tweet_fields="tweet.fields=created_at,author_id,geo"
    url = "https://api.twitter.com/2/tweets?{}&{}&{}&{}".format(ids,user_fields,tweet_fields, place_fields)   
    tweets=get_tweets(url)
    return tweets


#search_info=create_url("esquite","2023-03-29T00:00:00.000Z","2023-03-31T00:00:00.000Z",100)

search_info=create_url("esquite",100)
search_url = "https://api.twitter.com/2/tweets/search/recent"
params=search_info[1]    
headers = create_headers()

# Send the API request and get the response
tweets = connect_to_endpoint(search_url, params=params, headers=headers)
#print(tweets)

with open("query-data.json" , "w") as outfile:
    outfile.write(json.dumps(tweets,indent=4))

# Extract the IDs fo the tweets from the response
data=tweets["data"]
place_data=tweets["includes"]["places"]
user_data=tweets["includes"]["users"] #Location data from users is not 100% reliable
complete_data=[]
for tweet in data:
    id = tweet["id"]
    text = tweet["text"]
    author = tweet["author_id"]
    created_at = tweet["created_at"]
    location=extract_locationdata(author,user_data)
    row_data=[id,text,created_at,author,location]
    if "geo" in tweet.keys():
        place_id = tweet["geo"]["place_id"]
        geodata = extract_geodata(place_id,place_data)
        geodata.extend(place_id)
    else:
        geodata = ["NA",(0.0,0.0),"NA","NA"]
    row_data.extend(geodata)
    if row_data[4] != "" or row_data[5] != "NA":
        complete_data.append(row_data)

## Write the geodata and tweets to a CSV file
with open("datos_de_tweets.csv", "a", newline="") as csvfile:
    writer = csv.writer(csvfile)
    if csvfile.tell() == 0:
        writer.writerow(["Tweet id", "Texto","Fecha de Creación", "Id del autor", "Locación de la cuenta", "Locacion del tweet", "Coordenadas", "Tipo de Lugar", "id de locación"])
    writer.writerows(complete_data)

#ids="ids="
#geodata_ids=[]
#for tweet in data:
#    id = tweet["id"]
#    ids += id + ","
#ids=ids[:-1]
#tweet_data=get_tweets_by_ids(ids)
