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
client = MongoClient(uri, socketTimeoutMS=90000000, connectTimeoutMS=90000000)
db = client.twitterdb
collection = db.tweetsMexico

conceptos={
    'esquite':['esquite', 'trolelote', 'chasca', 'elote en vaso', 'vasolote', 'elote feliz', 'coctel de elote', 'elote desgranado'], 
    'bolillo':['bolillo', 'birote'], 
    'migaja':['migaja', 'borona', 'morona', 'morusa'], 
    'queso Oaxaca':['queso Oaxaca', 'quesillo', 'queso de hebra'], 
    'hormiga':['hormiga', 'asquel', 'asquiline', 'esquiline'], 
    'mosquito':['mosquito','zancudo','chaquiste','chanquiste','moyote'],
    'pavo':['pavo', 'guajolote', 'totole', 'totol', 'chompipe'], 
    'colibrí':['colibri', 'chupamirto', 'chuparrosa', 'chupaflor'], 
    'automóvil':['coche', 'automovil', 'carro', 'auto'], 
    'aguacero':['aguacero', 'chubasco', 'tormenta'], 
    'habitación':['habitacion', 'alcoba', 'dormitorio', 'recamara'], 
    'cobija':['cobija', 'frazada'], 
    'lentes':['lentes', 'anteojo', 'gafas', 'espejuelos'], 
    'itacate':['itacate', 'lunch', 'lonche', 'bastimento'], 
    'rasguño':['rasguño', 'arañazo'], 
    'lagaña':['legaña', 'lagaña', 'chinguiña'], 
    'comezón':['comezon', 'picazon', 'rasquera', 'rasquiña'],  
    'cinturón':['cinturon', 'cinto', 'fajo'],  #(bucar fajo con opción “sin billetes” en la expresión regular)
    'escusado':['retrete', 'escusado', 'inodoro', 'WC'], 
    'brasier':['brasier', 'chichero']  
}
def make_query(term):
    match term:
        case "chasca":
            regex = r"chas[ck]?a[s]?[\!\?]?[\s\w]*"
        case "elote en vaso":
            regex = r"elote[s]?[\s\w] en vaso[s]?[\!\?]?[\s\w]*"
        case "elote feliz":
            regex = r"elote[s]?[\s\w] feli[z]?[c]?[e]?[s]?[\!\?]?[\s\w]*"
        case "elote desgranado":
            regex = r"elote[s]?[\s\w] desgranado[s]?[\!\?]?[\s\w]*"
        case "coctel de elote":
            regex = r"coctel[e]?[s]?[\s\w] de elote[s]?[\!\?]?[\s\w]*"
        case "queso Oaxaca":
            regex = r"queso[s]?[\s\w] [Oo]?axaca*"
        case "queso de hebra":
            regex = r"queso[s]?[\s\w] de hebra*"
        case "chaquiste":
            regex = r"cha[n]?quiste[s]?[\!\?]?[\s\w]*"
        case "colibri":
            regex = r"colibr[ií]?[e]?[s]?[\!\?]?[\s\w]*"
        case "automovil":
            regex = r"autom[oó]?vil[e]?[s]?[\!\?]?[\s\w]*"
        case "habitacion":
            regex = r"habitaci[oó]?n[e]?[s]?[\!\?]?[\s\w]*"
        case "recamara":
            regex = r"rec[aá]?mara[s]?[\!\?]?[\s\w]*"
        case "comezon":
            regex = r"comez[oó]?n[e]?[s]?[\!\?]?[\s\w]*"
        case "picazon":
            regex = r"picaz[oó]?n[e]?[s]?[\!\?]?[\s\w]*"
        case "cinturon":
            regex = r"cintur[oó]?n[e]?[s]?[\!\?]?[\s\w]*"
        case "escusado":
            regex = r"e[sx]?cusado[s]?[\!\?]?[\s\w]*"
        case "WC":
            regex = r"\s+WC\s+"
        case "brasier":
            regex = r"bras[s]?ier[e]?[s]?[\!\?]?[\s\w]*"
        case "fajo":
            regex = r"fajo[s]?[\!,\?]?[\s\w]*(?!*\s*billetes)"
        case _:
            regex = fr"{term}[e]?[s]?[\!\?]?[\s\w]*"
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
        # Start a session
    with client.start_session() as session:
        # Set the session to be used for the following operations
        with session.start_transaction():# Start a session
            cursor = collection.find(my_query,  no_cursor_timeout=True)
    return cursor

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
        else:
                print('File already written. Skipping...')


HOME = os.environ["HOME"]
workdir=os.path.join(HOME,"repos/dialectic-twitter-maps-generator")
print(workdir)
datadir=os.path.join(workdir,'data')
for concept in conceptos:
    conceptdir=os.path.join(datadir,concept)
    # Execute the query and retrieve the matching tweets
    if not os.path.exists(conceptdir):
        os.makedirs(conceptdir)
        print(f"Making {conceptdir} directory...")
    for term in conceptos[concept]:
        print(f"Fetching database for tweets with {term}. This May take a while...")
        query=make_query(term)
        tweets = perform_mongo_query(query)
        filename = f"mongodb-{term}.csv"
        csv_file = os.path.join(conceptdir, filename)
        write_csv_data(csv_file, tweets)
        tweets.close()
        print(f"csv file {csv_file} written.")
    # Close the MongoDB connection
client.close()
