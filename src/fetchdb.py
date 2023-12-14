#!/usr/bin/python3
from pymongo import MongoClient
from datetime import datetime,  timedelta
from urllib.parse import quote_plus
import csv
import os
import pymongo
import logging
import time
import functools
import argparse

MONGO_DB = 'twitterdb'
MONGO_HOST = '132.247.22.53'
MONGO_USER = 'ConsultaTwitter'
MONGO_PASS = '$Con$ulT@C0V1D'
MONGO_MECHANISM = 'SCRAM-SHA-256'
MAX_AUTO_RECONNECT_ATTEMPTS = 5

uri = f'mongodb://{quote_plus(MONGO_USER)}:{quote_plus(MONGO_PASS)}@{MONGO_HOST}/{MONGO_DB}'
client = MongoClient(uri, socketTimeoutMS=90000000)
db = client.twitterdb
collection = db.tweetsMexico

conceptos={
    'esquite':['esquite', 'trolelote', 'chasca', 'chaska', 'elote en vaso', 'vasolote', 'elote feliz', 'coctel de elote', 'elote desgranado'], 
    'bolillo':['bolillo', 'birote'], 
    'migaja':['migaja', 'borona', 'morona', 'morusa'], 
    'queso Oaxaca':['queso Oaxaca', 'quesillo', 'queso de hebra'], 
    'hormiga':['hormiga', 'asquel', 'asquiline', 'esquiline'], 
    'mosquito':['mosquito', 'zancudo', 'chaquiste', 'chanquiste', 'moyote'], 
    'pavo':['pavo', 'guajolote', 'totole', 'totol', 'chompipe'], 
    'colibrí':['colibrí', 'chupamirto', 'chuparrosa', 'chupaflor'], 
    'automóvil':['coche', 'automóvil', 'carro', 'auto'], 
    'aguacero':['aguacero', 'chubasco', 'tormenta'], 
    'habitación':['habitación', 'alcoba', 'dormitorio', 'recámara'], 
    'cobija':['cobija', 'frazada'], 
    'lentes':['lentes', 'anteojo', 'gafas', 'espejuelos'], 
    'itacate':['itacate', 'lunch', 'lonche', 'bastimento'], 
    'rasguño':['rasguño', 'arañazo'], 
    'lagaña':['legaña', 'lagaña', 'chinguiña'], 
    'comezón':['comezón', 'picazón', 'rasquera', 'rasquiña'],  
    'cinturón':['cinturón', 'cinto', 'fajo'],  #(bucar fajo con opción “sin billetes” en la expresión regular)
    'escusado':['retrete', 'escusado/excusado', 'inodoro', 'WC'], 
    'brasier':['brasier', 'brassier', 'chichero']  
}
def make_query(concept):
    if concept == "WC":
        regex=r" " + concept + " "
    else:
        regex=r"" + concept + "[s]?[\!\?]?[\s\w]*"
    query = {
        "$or" : [
            {"place": {"$ne": None}}, 
                {"geo": {"$ne": None}}, 
        ], 
        "text": {"$regex": regex,  "$options": "i"}
        }
    return query

def graceful_auto_reconnect(mongo_op_func):
    """Gracefully handle a reconnection event."""
    @functools.wraps(mongo_op_func)
    def wrapper(*args,  **kwargs):
        for attempt in range(MAX_AUTO_RECONNECT_ATTEMPTS):
            try:
                return mongo_op_func(*args,  **kwargs)
            except pymongo.errors.AutoReconnect as e:
                wait_t = 0.5 * pow(2,  attempt) # exponential back off
                logging.warning("PyMongo auto-reconnecting... %s. Waiting %.1f seconds.",  str(e),  wait_t)
                time.sleep(wait_t)

        raise Exception(f"Failed after {MAX_AUTO_RECONNECT_ATTEMPTS} attempts.")
    return wrapper

# Your MongoDB query function that you want to handle gracefully
@graceful_auto_reconnect
def perform_mongo_query(my_query):
    print(f"Performing query for {my_query}")
    return collection.find(my_query,  no_cursor_timeout=True)

# Specify the CSV file path
def write_csv_data(csv_file_path, data):
    with open(csv_file_path, "a", newline="", encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if csvfile.tell() == 0:
            # If the file is empty,  write the header row
            writer.writerow(['Tweet', 'Fecha', 'Locacion', 'Longitud(V1)', 'Latitud(V1)', 'Longitud(V2)', 'Latitud(V2)', 'Longitud(V3)', 'Latitud(V3)', 'Longitud(V4)', 'Latitud(V4)'])

        for tweet in data:
            placedata = tweet['place']
            data = [tweet['text'], tweet['created_at'], placedata['full_name']]
            vertices = placedata['bounding_box']['coordinates'][0]
            cordenadas = []
            for vertex in range(len(vertices)):
                cordenadas.extend([vertices[vertex][0], vertices[vertex][1]])
            data.extend(cordenadas)
            writer.writerow(data)
    print(f'Tweets saved to {csv_file_path}...')

parser=argparse.ArgumentParser(description='Make database .csv file from the fetched database per word, this includes the geodata and tweets')
parser.add_argument('concepto',metavar='concept',type=str)
parser.parse_args(['-'])
cl_argument=parser.parse_args()
concept=cl_argument.concepto

cwd=os.getcwd()
datadir=os.path.join(cwd,'data')
conceptdir=os.path.join(datadir,concept)
# Execute the query and retrieve the matching tweets
if (concept in conceptos):
    if not os.path.exists(conceptdir):
        os.makedirs(conceptdir)
        print(f"Making {conceptdir} directory...")
    for term in conceptos[concept]:
        query=make_query(term)
        tweets = perform_mongo_query(query)
        filename = f"mongodb-{term}.csv"
        csv_file = os.path.join(conceptdir, filename)
        write_csv_data(csv_file, tweets)
        tweets.close()
        print(f"csv file {csv_file} writen.")
else:
    print(f"""Sorry concept '{concept}' not in list.""")
# Close the MongoDB connection
client.close()
