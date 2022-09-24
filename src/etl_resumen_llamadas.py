#seudocodigo
#1. leer el archivo csv
#2. Extraer el resumen
#3. Guardar el resumen en formato csv


import pandas as pd
import os
from pathlib import Path


def main():
    #leer archivo
    filename="llamadas123_julio_2022.csv"
    data= get_data(filename= filename)
    #extrae resumen
    df_resumen = get_summary(data)
    #guardar resumen
    save_data(df_resumen, filename)

def save_data(df, filename):
    out_name= 'resumen2_' + filename
    root_dir= Path(".").resolve()
    out_path = os.path.join(root_dir,"data", "processed",out_name)
    df.to_csv(out_path)

def get_summary(data):
    dic_resume = dict()
    for col in data.columns:
        valores_unicos =data[col].unique()
        n_valores = len(valores_unicos)
        dic_resume[col]= n_valores
    df_resumen=pd.DataFrame.from_dict(dic_resume,orient='index')
    df_resumen.rename({0:'Count'}, axis=1, inplace=True)
    return df_resumen


def get_data(filename):
    data_dir="raw"
    root_dir =Path(".").resolve()
    file_path =os.path.join(root_dir, "data", data_dir, filename)
    data=pd.read_csv(file_path, encoding='latin-1',sep=';')
    

    return data

if __name__ == '__main__':
    main()