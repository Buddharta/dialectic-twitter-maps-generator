#!/usr/bin/python3
import csv
import os
import argparse
import pandas as pd

corr_estados={"Coahuila de Zaragoza":"Coahuila", "México" : "Estado de México", "Michoacán de Ocampo" : "Michoacán", "Veracruz de Ignacio de la Llave":"Veracruz"}
estados=["Aguascalientes", "Baja California", "Baja California Sur", "Campeche", "Chiapas", "Chihuahua", "Ciudad de México", "Coahuila de Zaragoza", "Colima", "Durango", "México", "Guanajuato", "Guerrero", "Hidalgo", "Jalisco", "Michoacán de Ocampo", "Morelos", "Nayarit", "Nuevo León", "Oaxaca", "Puebla", "Querétaro", "Quintana Roo", "San Luis Potosí", "Sinaloa", "Sonora", "Tabasco", "Tamaulipas", "Tlaxcala", "Veracruz de Ignacio de la Llave", "Yucatán", "Zacatecas"]
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

DATA_DIR="/home/shakya/source/PYTHON/tweet-scrape/data"
OUT_DIR="/home/shakya/source/PYTHON/tweet-scrape/outputs"

def dictsum(dic2: dict, dic1 :dict) -> dict:
    return {key: dic1.get(key, 0) + dic2.get(key, 0) for key in set(dic1) | set(dic2)}

def get_location_data(file) -> dict:
    df = pd.read_csv(file,dtype={"Estado": str,"Poblacion": int,})
    apariciones=dict.fromkeys(estados,0)
    total=len(list(df["Estado"]))
    apariciones['Total']=total
    for item in df["Estado"]:
        apariciones[item]+=1
    return apariciones

def create_csv_files(concept :str):
    if concept == 'escusado':
        escusado_file_path=os.path.join(DATA_DIR,f"escusado/db-escusado-fixed.csv")
        excusado_file_path=os.path.join(DATA_DIR,f"escusado/db-excusado-fixed.csv")
        escusado_data=get_location_data(escusado_file_path)
        excusado_data=get_location_data(excusado_file_path)
        ex_or_es_escusado_data=dictsum(escusado_data,excusado_data)
    
    for palabra in conceptos[concept]:
        concept_fname= "escusado-map.csv" if palabra == 'escusado/excusado' else f"{palabra}-map.csv"
        concept_file=os.path.join(OUT_DIR,concept_fname)
        with open(concept_file, "a", newline="", encoding='utf-8') as wordfile:
            if wordfile.tell() == 0:
                if palabra == 'escusado/excusado':
                    data_by_concept=ex_or_es_escusado_data  
                else:
                    dbfile_path=os.path.join(DATA_DIR,f"{concept}/db-{palabra}-fixed.csv")
                    print(f"Writing {concept_fname}.csv file...")
                    data_by_concept=get_location_data(dbfile_path)
                
                writer = csv.writer(wordfile)
                writer.writerow(['Estado','Ocurrencias'])
                for value in data_by_concept.items():
                    if value[0] in corr_estados.keys():
                        correctos=[corr_estados[value[0]],value[1]]
                        writer.writerow(correctos)
                    else:
                        writer.writerow(list(value))
            else:
                print(f"File {concept_file} already written, skiping...")
                pass

    new_file_name=f"{concept}.csv"
    new_csv_file_path=os.path.join(OUT_DIR, new_file_name)
    
    with open(new_csv_file_path, "a", newline="", encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        if outfile.tell() == 0: # If the file is empty,  write the header row
            write_data=[]
            header=["Estado"]
            header.extend(conceptos[concept])
            writer.writerow(header)
            for palabra in conceptos[concept]:
                if palabra == 'escusado/excusado':
                    data_by_concept=ex_or_es_escusado_data  
                else:
                    file_path=os.path.join(DATA_DIR,f"{concept}/db-{palabra}-fixed.csv")
                    print(f"Processing {file_path} data...")
                    data_by_concept=get_location_data(file_path)
                write_data.append(data_by_concept)
            for estado in estados:
                row_data=[estado]
                ocurrence_data=[d[estado] for d in write_data]
                row_data.extend(ocurrence_data)
                writer.writerow(row_data)
            final=['Total']
            totales=[d['Total'] for d in write_data]
            final.extend(totales)
            writer.writerow(final)
        else:
            print(f"File {concept}.csv already written")

gabmap=True
parser=argparse.ArgumentParser(description='Make the conceptos.csv file from the fetched database files')
parser.add_argument('concepto',metavar='concept',type=str)
parser.add_argument('--gabmap',dest='gabmap',action='store_const',
                    const=gabmap, default=False,
                    help='Make the corresponding file to be processed by gabmap')
parser.parse_args(['-'])
cl_argument=parser.parse_args()
concept=cl_argument.concepto
gabmap_file_data=os.path.join(OUT_DIR,f'outputs/{concept}-gabmapdata.csv')
if (concept in conceptos):
    create_csv_files(concept)
else:
    print(f"""Sorry concept '{concept}' not in list.""")

##def get_places():
##    places=set()
##    for file in datafiles:
##        filepath=os.path.join(datadir,file)
##        with open(filepath,'r') as fl:
##            csv_reader=csv.DictReader(fl)
##            for row in csv_reader:
##                places.add(row['Locacion'])
##    return list(places)
##lugares=get_places()
##
##
##with open(outfile, "w", newline="",encoding='utf-8') as csvfile:
##    writer = csv.writer(csvfile)
##    if csvfile.tell() == 0:
##        # If the file is empty, write the header row
##        keys=['Palabra','Total de ocurrencias']
##        keys.extend(lugares)
##        writer.writerow(keys)
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
##
##with open(outdata, "w", newline="",encoding='utf-8') as csvfile:
##    writer = csv.writer(csvfile)
##    if csvfile.tell() == 0:
##        # If the file is empty, write the header row
##        keys=['Locacion','Retrete']
##        writer.writerow(keys)
##
##    with open(outfile,'r') as file:
##        csv_reader=csv.DictReader(file)
##        variantes={lugar:'' for lugar in lugares}
##        for row in csv_reader:
##            for lugar in lugares:
##                if int(row[lugar]) > 0:
##                    variantes[lugar]+=row['Palabra'] + " / "
##        for lugar in lugares:
##            data=[lugar,variantes[lugar][:-3]]
##            writer.writerow(data)
##

