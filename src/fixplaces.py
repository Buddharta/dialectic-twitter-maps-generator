import numpy as np
import json
import csv
import os
import collections

cwd=os.getcwd()
datadir=os.path.join(cwd,'data')
outfile=os.path.join(cwd,'outputs/out.csv')
outdata=os.path.join(cwd,'outputs/gabmapdata.csv')
datafiles=os.listdir(datadir)
KEYVALUES = ['NOM_ENT','NOM_MUN','LAT_DECIMAL','LON_DECIMAL','POB_TOTAL']

dist = lambda x,y: math.sqrt(x*x + y*y)

def get_key_value(key): 
    strn=""
    match key:
        case 'NOM_ENT':
            strn+="Entidad"
        case 'NOM_MUN':
            strn+="Municipio"
        case 'LAT_DECIMAL':
            strn+="Latitud"
        case 'LON_DECIMAL':
            strn+="Longitud"
        case 'POB_TOTAL':
            strn+="Poblacion"
        case _:
            strn+="NA"
    return strn

def get_coordinates():
    coordinates=[]
    for file in datafiles:
        filepath = os.path.join(datadir,file)
        with open(filepath,'r') as fle:
            csv_reader = csv.DictReader(fle)            
            for row in csv_reader:
                coord1=np.array([float(row['Longitud(V1)']),float(row['Latitud(V1)'])],dtype=float)
                coord2=np.array([float(row['Longitud(V2)']),float(row['Latitud(V2)'])],dtype=float)
                coord3=np.array([float(row['Longitud(V3)']),float(row['Latitud(V3)'])],dtype=float)
                coord4=np.array([float(row['Longitud(V4)']),float(row['Latitud(V4)'])],dtype=float)
                barycenter = 0.25*(coord1+coord2+coord3+coord4)
                coordinates.append(barycenter)
    return coordinates

## FUNCION POR ACABAR, ESTA FUNCION DEBE DE DAR UNA LISTA DE TUPAS (KET,LIST_VALUE) 
## DONDE LIST_VALUE ES LA LISTA DE VALORES ASOCIADOS A LA LLAVE KEY 
def get_AGEEML_2023821859377_location_data():
    places = set()
    ldata=()
    file=os.path.join(datadir,'Extra/AGEEML_2023821859377.csv')
    with open(file,'r') as fl:
        csv_file=csv.DictReader(fl)
        datalist = []
        for key,value in csv_file.items():
            if key in KEYVALUES:
                newkey=get_key_value(key) 
                datalist.append(value)
        ldata.fromkeys(places,datalist) 
    return ldata
lugares=get_places()


##with open(outfile, "w", newline="",encoding='utf-8') as csvfile:
##    writer = csv.writer(csvfile)
##    if csvfile.tell() == 0:
##        # If the file is empty, write the header row
##        keys=['Palabra','Total de ocurrencias']
##        keys.extend(lugares)
##        writer.writerow(keys)
##
##
##    for file in datafiles:
##        ocurrencias={lugar:0 for lugar in lugares}
##        palabra=file[0:-4]
##        filepath=os.path.join(datadir,file)
##        with open(filepath,'r') as fl:
##            csv_reader=csv.DictReader(fl)
##            for row in csv_reader:
##                ocurrencias[row['Locacion']]+=1
##
##            valores=list(ocurrencias.values())
##            totales=sum(valores)
##            data=[palabra,totales]
##            data.extend(valores)
##            writer.writerow(data)
##
