#importamos las librerias necesarias

import pandas as pd
import numpy as np
from pathlib import Path
import os
from dateutil.parser import parse
#libreria dateutil permite ayuda con formatos fecha


#definimos la funcion main:clean_process

def main():
    print("Ingrese el nombre del archivo:")
    #filename=input()
    filename="llamadas123_julio_2022.csv"
    #Obtenemos la data
    data=get_data(filename =filename)
    print("Shape:",data.shape)
    #Eliminamos registros completos duplicados
    data=dropduplicates(data=data)
    print("Shape:",data.shape)
    #Formatos y valores de columnas
    data=formatvalues(data= data)
    #guardamos el archivo
    savefile(data=data,filename=filename)
    print("Archivo Limpio y Guardado")
    

def get_data(filename):
    root_dirpath=Path(".").resolve().parent
    filepath=os.path.join(root_dirpath,"data","raw",filename)
    data=pd.read_csv(filepath, encoding='latin-1', sep=';')
    return data

def dropduplicates(data):
    data=data.drop_duplicates()
    data=data.reset_index()
    return data

def formatvalues(data):
    print(data.info())
    print("Valores UNIDAD:",data['UNIDAD'].value_counts(dropna=False))
    data['UNIDAD']=data['UNIDAD'].fillna('SIN_DATO') #los valores NaN, le asignamos SIN_DATO
    print("Valores UNIDAD:",data['UNIDAD'].value_counts(dropna=False))
    #formateamos la fecha ya que esta en string
    col='FECHA_INICIO_DESPLAZAMIENTO_MOVIL'
    data[col]=pd.to_datetime(data[col],errors='coerce')
    col='RECEPCION'
    list_fechas =list()#creamos una lista para almacenar de manera paralela al loop en el orden correcto la fechas convertidas
    print(data.shape[0])
    n_rows=data.shape[0] #len(data[col])
    for i in range(0,n_rows):
        try:
            strfecha=data[col][i]
            f=datestring(strfecha)
            list_fechas.append(f)
        except Exception as e:
            list_fechas.append(strfecha)
        continue
    data["RECEPCION_CORREGIDA"] = list_fechas
    data["RECEPCION_CORREGIDA"] = pd.to_datetime(data['RECEPCION_CORREGIDA'], errors='coerce')
    # Reemplazar sin dato con un valor nulo de tipo numerico
    data['EDAD']=data['EDAD'].replace({'SIN_DATO':np.nan})
    #funciones lambda en python
    x='3'
    f=lambda x: x if pd.isna(x)==True else int(x)
    f(x)
    data['EDAD']=data['EDAD'].apply(f)
    print(data.info())
    return data
#una funcion que reciba un string, con fechas y regrese un valor en datetime usando parse
def datestring(dateS):
    x=parse(dateS, dayfirst=True)
    return x

def savefile(data,filename):
    outname="clean_"+ filename
    root_dir=Path(".").resolve().parent
    filepath=os.path.join(root_dir,"data","processed",outname)
    data.to_csv(filepath)


if __name__ == '__main__':
    main()
