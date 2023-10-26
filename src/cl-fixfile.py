import numpy as np
import csv
import os
import argparse

parser=argparse.ArgumentParser(description='Fix place data of given files in the database.')
parser.add_argument('file', type=argparse.FileType('r'))
parser.parse_args(['-'])
datafile=parser.parse_args()
openedfile=datafile.file
file_path=vars(datafile)['file'].name
fname=os.path.basename(file_path)
print(f"Processing {fname}...")

cwd=os.getcwd()
datadir=os.path.join(cwd,'data')

AGEEML_DATA={}
AGEEML_file=os.path.join(datadir,'Extra/AGEEML_2023821859377.csv')
with open(AGEEML_file,'r') as fl:
    csv_file=csv.DictReader(fl)
    dictlist=list(csv_file)
    AGEEML_DATA={
    'Ambito':[data['AMBITO'] for data in dictlist],
    'Estado':[f"{data['NOM_ENT']}" for data in dictlist],
    'Nombre':[f"{data['NOM_MUN']}, {data['NOM_LOC']}" for data in dictlist],
    'Coordenadas':[np.array([data['LON_DECIMAL'],data['LAT_DECIMAL']],dtype=float) for data in dictlist],
    'Poblacion':[0 if data['POB_TOTAL'] == "-" else int(data['POB_TOTAL']) for data in dictlist ]
    }

def closest_AGEEML_2023821859377_location(coordinates) -> list:
    min=1000
    index=0
    pindex=0
    for point in AGEEML_DATA['Coordenadas']:
        distance=np.linalg.norm(coordinates-point)
        if distance < min:
            min=distance
            pindex=index
        index+=1
        if min < 0.01:
            break
    closest_place_data=[AGEEML_DATA['Estado'][pindex], AGEEML_DATA['Nombre'][pindex], AGEEML_DATA['Ambito'][pindex], coordinates[0], coordinates[1], AGEEML_DATA['Poblacion'][pindex]]
    return closest_place_data

def fix_places(fle,name :str):
    csv_reader = csv.DictReader(fle)
    new_file_name=f"{name[5:-4]}-fixed.csv"
    new_csv_file_path=os.path.join(datadir, new_file_name)
    with open(new_csv_file_path, "a", newline="", encoding='utf-8') as newcsvfile:
        writer = csv.writer(newcsvfile)
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

fix_places(openedfile,fname)
