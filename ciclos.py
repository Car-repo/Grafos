#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 5 ‏‎13:58:36 2022

@author: mat
"""
import sys
import pyspark
SparkContext = pyspark.SparkContext

a = SparkContext.getOrCreate().stop(); del a
sc = SparkContext(appName='Ciclos')


def unir(lista, local=False):
    """
    Lee una lista de archivos y los junta en un rdd. Tambien renombra vertices
    para obtener ciclos locales. El subindice i-esimo corresponde con el archivo
    i-esimo, numerados de 1 a n.
    """
    
    sc = SparkContext.getOrCreate()
    
    if not isinstance(lista, list):
        lista = [lista]
        
    if local:
        j = 1
        sol = formatear(sc.textFile(lista[0]), i=f'_{j}')
        for nombre in lista[1:]:
            j += 1
            rdd = formatear(sc.textFile(nombre),i=f'_{j}')
            sol = sol.union(rdd)     
            
    else:
        sol = formatear(sc.textFile(lista[0]))
        for nombre in lista[1:]:
            rdd = formatear(sc.textFile(nombre))
            sol = sol.union(rdd)

    return sol


def formatear(rdd, i=''):
    """
    Lee el rdd por filas y añade una etiqueta a cada vertice si fuese necesario.
    """
    
    dat = rdd.map(lambda x: tuple(x.strip().split(','))).\
    map(lambda x: (x[0]+i, x[1]+i))
    
    return dat


def adyacente(rdd):
    """
    Ordena los vertices de las aristas y los agrupa por llave. Asi 
    el resultado es (vertice, adyacencia).
    """

    sol = rdd.map(lambda x: tuple(sorted(x))).groupByKey().mapValues(list)

    return sol.sortByKey()


def pending_exists(a):
    """
    A partir de un par (vertice, adyacencia), obtiene la lista de aristas que existen
    o que querriamos que existiesen.
    """
    
    l1=[((a[0], j), 'exists') for j in a[1]]
    
    l2=[((e1,e2), ('pending',a[0])) for e1 in a[1] for e2 in a[1] if e1 < e2]
    
    return l1+l2


def reduccion(data):
    """
    Obtiene los ciclos.
    """
    
    tupla, lista = data
    
    return [(elem[1], *tupla) for elem in lista if elem != 'exists']


def computo_ciclos(lista, local=False):
    
    raw_data = unir(lista, local)
    data = adyacente(raw_data)
    
    data = data.flatMap(pending_exists)
    
    ciclos = data.groupByKey().mapValues(list).\
    filter(lambda x: len(x[1])>1).flatMap(reduccion).\
    sortBy(lambda x: x[0])
    
    if local:
        rango = range(len(lista))
        print(dict([(lista[j], j+1) for j in rango]))

    return ciclos



if __name__ == '__main__':
    """
    Se especifica si los ciclos se calculan localmente justo al final. 
    """
    
    if len(sys.argv) > 1:
        b = sys.argv[-1]
        if b.lower() == 'true':
            rdd = computo_ciclos(sys.argv[1:-1],True)
            print(rdd.collect())
        elif b.lower() == 'false':
            rdd = computo_ciclos(sys.argv[1:-1])
            print(rdd.collect())
        else:
            rdd = computo_ciclos(sys.argv[1:])
            print(rdd.collect())
    else:
        print('\nArgs: file_1.txt file_2.txt ... file_m.txt booleano',
              '\nEl booleano es parametro opcional, por defecto es False')
