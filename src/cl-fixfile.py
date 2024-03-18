#!/usr/bin/env python
import pandas as pd
import numpy as np
import csv
import os
import argparse
import io
from scipy.spatial import KDTree
from contextlib import redirect_stdout

parser=argparse.ArgumentParser(description='Fix place data of given files in the database.')
parser.add_argument('file', type=argparse.FileType('r'))
parser.parse_args(['-'])
datafile=parser.parse_args()
openedfile=datafile.file
file_path=vars(datafile)['file'].name
fname=os.path.basename(file_path)
file_dir=os.path.dirname(file_path)
print(f"File path: {file_dir}")
print(f"Processing {fname}...")

HOME=os.environ["HOME"]
DATA_DIR=os.path.join(HOME,"repos/dialectic-twitter-maps-generator/data")
AGEEML_file=os.path.join(DATA_DIR,'Extra/AGEEML_2023821859377.csv')
AGEEML_data=pd.read_csv(AGEEML_file,
                        dtype={'NOM_ENT':str,'NOM_MUN':str,'NOM_LOC':str,'AMBITO':str,'LON_DECIMAL':np.float16,'LAT_DECIMAL':np.float16,'POB_TOTAL':str})

def closest_AGEEML_2023821859377_location(coordinates) -> list:
    tree = KDTree(AGEEML_data[['LON_DECIMAL', 'LAT_DECIMAL']])
    # Query the KDTree to find the index of the closest point
    _, min_index = tree.query(coordinates)
    # Retrieve the row with minimum distance
    closest_location = AGEEML_data.iloc[min_index]
    closest_place_data=[AGEEML_data['NOM_ENT'][min_index], AGEEML_data['NOM_MUN'][min_index], AGEEML_data['AMBITO'][min_index], coordinates[0], coordinates[1], AGEEML_data['POB_TOTAL'][min_index]]
    return closest_place_data

def fix_places(data_dir,name :str) -> int:
    print(f"{name} loaded...")
    if name.startswith('mongodb'):
        file=os.path.join(data_dir,name)
        new_file_name=f"{name[5:-4]}-fixed.csv"
        new_csv_file_path=os.path.join(data_dir, new_file_name)
        with open(file, newline='') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            with open(new_csv_file_path, "a", newline="", encoding='utf-8') as newcsvfile:
                writer = csv.writer(newcsvfile)
                print(f"Writing new file {new_file_name} in {data_dir}")
                if newcsvfile.tell() == 0: # If the file is empty,  write the header row
                    writer.writerow(['Tweet', 'Fecha', 'Estado', 'Locacion', 'Tipo' ,'Longitud', 'Latitud', 'Poblacion'])
                    for row in csv_reader:
                        row_data=[row['Tweet'],row['Fecha']]
                        coord1=np.array([float(row['Longitud(V1)']),float(row['Latitud(V1)'])],dtype=float)
                        coord2=np.array([float(row['Longitud(V2)']),float(row['Latitud(V2)'])],dtype=float)
                        coord3=np.array([float(row['Longitud(V3)']),float(row['Latitud(V3)'])],dtype=float)
                        coord4=np.array([float(row['Longitud(V4)']),float(row['Latitud(V4)'])],dtype=float)
                        coordinates = 0.25*(coord1+coord2+coord3+coord4)
                        placedata=closest_AGEEML_2023821859377_location(coordinates)
                        row_data.extend(placedata)
                        writer.writerow(row_data)
                    return 0
                else:
                    print(f"File {name} already fixed")
                    return 1

    else:
        print("Incompatible file or already fixed file")
        return 2

f = io.StringIO()
with redirect_stdout(f):
    fix_places(file_dir,fname)
out = f.getvalue()
