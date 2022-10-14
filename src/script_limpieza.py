#importamos las librerias necesarias

import pandas as pd
import numpy as np
from pathlib import Path
import os
from dateutil.parser import parse

#libreria dateutil permite ayuda con formatos fecha


#definimos la funcion main:clean_process

bucket='gs://martinez_bucket_llamadas123'


def main():
    print("Ingrese el mes:")
    mes=input()
    #filename=f"llamadas_123_-{mes}2021.csv" #Enero2021
    #filename=f"llamadas_123_{mes}2021.csv" #Febrero2021 hasta Septiembre2021
    #filename=f"llamadas_123_{mes}_2021.csv" #Octubre2021 hasta Diciembre2021
    #filename=f"datos_abiertos_{mes}_2022.csv" #enero2022 hasta abril2022
    #filename=f"datos_llamadas123_{mes}_2022.csv" #mayo2022
    filename=f"llamadas123_{mes}_2022.csv" #desde junio2022 
    ano=filename[-8:-4]
    
    #Obtenemos la data
    data=get_data(filename =filename)
    print("Shape:",data.shape)
    #Eliminamos registros completos duplicados
    data=dropduplicates(data=data)
    print("Shape:",data.shape)
    #Formatos y valores de columnas
    data=formatvalues(data= data,mes=mes,ano=ano)
    #guardamos el archivo
    savefile(data=data,filename=filename,mes=mes,ano=ano)
    print("Archivo Limpio y Guardado")
    

def get_data(filename):
    
    #root_dirpath=Path(".").resolve().parent
    filepath=os.path.join(bucket,"data","raw",filename)
    data=pd.read_csv(filepath, encoding='latin-1', sep=';')
    
    if data.columns[0]!='NUMERO_INCIDENTE':
        data.columns.values[0]='NUMERO_INCIDENTE'
    if data.columns[1]!='FECHA_INICIO_DESPLAZAMIENTO_MOVIL':
        data.columns.values[1]='FECHA_INICIO_DESPLAZAMIENTO_MOVIL'
    if data.columns[2]!='CODIGO_LOCALIDAD':
        data.columns.values[2]='CODIGO_LOCALIDAD'
    if data.columns[3]!='LOCALIDAD':
        data.columns.values[3]='LOCALIDAD' 
    
    return data

def dropduplicates(data):
    data=data.drop_duplicates()
    data=data.reset_index()
    return data

def formatvalues(data,mes,ano):
    data['CODIGO_LOCALIDAD'] = pd.to_numeric(data['CODIGO_LOCALIDAD'], downcast='integer',errors='coerce')
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
    strfecha=''
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
    data['EDAD']=data['EDAD'].replace({'SIN_DATO':np.nan,'Sin_dato':np.nan})
    #funciones lambda en python
    x='3'
    f=lambda x: x if pd.isna(x)==True else int(x)
    f(x)
    data['EDAD']=data['EDAD'].apply(f)
    data['CODIGO_LOCALIDAD']=pd.Series(data['CODIGO_LOCALIDAD'],dtype="Int64")
    data['EDAD']=pd.Series(data['EDAD'],dtype="Float64")
    data.rename(columns={'Unnamed: 10':'unnamed10','Unnamed: 11':'unnamed11'},inplace=True)
    if (mes=='octubre') & (ano=='2021'):
            
        data.drop('unnamed10',axis=1,inplace=True)
        data.drop('unnamed11',axis=1,inplace=True)
            
    if len(data['RECEPCION'])>0:
        data.drop('RECEPCION',axis=1,inplace=True)
            
    print(data.info())
    return data
#una funcion que reciba un string, con fechas y regrese un valor en datetime usando parse
def datestring(dateS):
    x=parse(dateS, dayfirst=True)
    return x

def savefile(data,filename,mes,ano):
    filename2=filename
    if filename==filename2:
        filename=f"llamadas123_{mes}_{ano}.csv"
    else:
        filename=filename
    outname="clean_"+ filename
    
    #root_dir=PATH(".").resolve().parent
    filepath=os.path.join(bucket,"data","processed",'limpieza123',outname)
    data.to_csv(filepath)
    print(filepath)
    #Guardar Tabla en Big Query
    data.to_gbq(destination_table='llamadas123_data_2022_2021.llamadas_Limpieza_123',if_exists='append')


if __name__ == '__main__':
    main()
