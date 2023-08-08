import json
import csv
import os
import collections

cwd=os.getcwd()
datadir=os.path.join(cwd,'data')
outfile=os.path.join(cwd,'outputs/out.csv')
outdata=os.path.join(cwd,'outputs/gabmapdata.csv')
datafiles=os.listdir(datadir)

conceptos={
    'esquite':['esquite','trolelote','chasca','chaska','elote en vaso','vasolote','elote feliz','coctel de elote','elote desgranado'],
    'bolillo':['bolillo','birote'],
    'migaja':['migaja','borona','morona','morusa'],
    'queso Oaxaca':['queso Oaxaca','quesillo','queso de hebra'],
    'hormiga':['hormiga','asquel','asquiline','esquiline'],
    'mosquito':['mosquito','zancudo','chaquiste','chanquiste','moyote'],
    'pavo':['pavo','guajolote','totole','totol','chompipe'],
    'colibrí':['colibrí','chupamirto','chuparrosa','chupaflor'],
    'automóvil':['coche','automóvil','carro','auto'],
    'aguacero':['aguacero','chubasco','tormenta'],
    'habitación':['habitación','alcoba','dormitorio','recámara'],
    'cobija':['cobija','frazada'],
    'lentes':['lentes','anteojo','gafas','espejuelos'],
    'itacate':['itacate','lunch','lonche','bastimento'],
    'rasguño':['rasguño','arañazo'],
    'lagaña':['legaña','lagaña','chinguiña'],
    'comezón':['comezón','picazón','rasquera','rasquiña'], 
    'cinturón':['cinturón','cinto','fajo'], #(bucar fajo con opción “sin billetes” en la expresión regular)
    'retrete':['retrete','excusado','sanitario','inodoro','escusado','WC'],
    'brasier':['brasier','brassier','chichero']  
}

def get_places():
    places=set()
    for file in datafiles:
        filepath=os.path.join(datadir,file)
        with open(filepath,'r') as fl:
            csv_reader=csv.DictReader(fl)
            for row in csv_reader:
                places.add(row['Locacion'])
    return list(places)
lugares=get_places()


with open(outfile, "w", newline="",encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    if csvfile.tell() == 0:
        # If the file is empty, write the header row
        keys=['Palabra','Total de ocurrencias']
        keys.extend(lugares)
        writer.writerow(keys)

    for file in datafiles:
        ocurrencias={lugar:0 for lugar in lugares}
        palabra=file[0:-4]
        filepath=os.path.join(datadir,file)
        with open(filepath,'r') as fl:
            csv_reader=csv.DictReader(fl)
            for row in csv_reader:
                ocurrencias[row['Locacion']]+=1

            valores=list(ocurrencias.values())
            totales=sum(valores)
            data=[palabra,totales]
            data.extend(valores)
            writer.writerow(data)


with open(outdata, "w", newline="",encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    if csvfile.tell() == 0:
        # If the file is empty, write the header row
        keys=['Locacion','Retrete']
        writer.writerow(keys)

    with open(outfile,'r') as file:
        csv_reader=csv.DictReader(file)
        variantes={lugar:'' for lugar in lugares}
        for row in csv_reader:
            for lugar in lugares:
                if int(row[lugar]) > 0:
                    variantes[lugar]+=row['Palabra'] + " / "
        for lugar in lugares:
            data=[lugar,variantes[lugar][:-3]]
            writer.writerow(data)


