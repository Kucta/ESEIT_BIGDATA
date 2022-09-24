import numpy as np
import argparse
import pandas as pd
def calcularValores(lista_numeros,verbose):
    """Retorna los valores de suma,media, desviacion estandar y minimos y maximos

    Args:
        lista_numeros (list): lista con valores numericos
        verbose (bool): para decidir imprimir mensajes en pantalla. Defaults to False.

    Returns:
        tupla: ((min,max),(media,desviacionestandar),suma)
    """
    #get_maxmin(list_num)
    valMaxMin =calcular_min_max(lista_numeros,verbose)
    #get_medias(lis_num)
    medDesvStd =valorescentrales(lista_numeros,verbose)
    #get_sum(list_num)
    suma=np.sum(lista_numeros)
    #return sum, min_val, max_val,media, dev_stan
    return valMaxMin,medDesvStd,suma


#get_maxmin(list_num)
def calcular_min_max(lista_numeros, verbose = False):
    '''
    Funcion que calcula los minimos y maximos de una lista
    
    '''
    min_value=min(lista_numeros)
    max_value=max(lista_numeros)
    if verbose == True:
        print("Minimo: ",min_value)
        print("Maximo: ",max_value)
    else:
        pass
    return min_value,max_value
#get_medias(lis_num)
def valorescentrales(lista_numeros, verbose = False):
    """Calcula la media y la desviacion estandar de una lista de numeros

    Args:
        lista_numeros (list): lista con valores numericos
        verbose (bool, optional): para decidir imprimir mensajes en pantalla. Defaults to False.

    Returns:
        tupla: (media,desviacion estandar)
    """
    md=np.mean(lista_numeros)
    desv_std=np.std(lista_numeros)
    if verbose ==True:
        print("Meida",md)
        print("Desviacion",desv_std)
    return md, desv_std

def main():
    parser =argparse.ArgumentParser()
    parser.add_argument("--verbose", type=bool, default=True, help="Para imprimir en pantalla")
    args=parser.parse_args()

    lista_valores=[5,4,8,9,21]
    calcularValores(lista_valores,True)

if __name__ == '__main__':
    main()