#!/usr/bin/env python
'''
ETL [Python]
Ejemplos de clase
---------------------------
Autor: Inove Coding School
Version: 1.1

Descripcion:
Programa de pruebas con JSON y requests
'''

import csv
import os
import requests
from datetime import datetime

import numpy as np
import matplotlib
matplotlib.use('Agg')   # For multi thread, non-interactive backend (avoid run in main loop)
import matplotlib.pyplot as plt
import bonobo

# Install bonobo
#   pip3 install -U bonobo
# Crear archivo "dot"
#   bonobo inspect --graph 3_etl_json.py > 3_etl_json.dot
# Graphviz online
#   http://dreampuf.github.io/GraphvizOnline/
# Graphviz (dot) extension
#    Abrir el archivo .dot y presionar CTRL + SHIF + V


API_URL = 'http://inove.pythonanywhere.com/finanzas'

def extract():
    # Leer todo el JSON junto, se envia la siguiente
    # proceso todo el json.
    yield requests.get(API_URL).json()

    
def transform(dataset):
    # Del dataset solo me quedo con la columna de los minutos.
    # y el precio de cierre de la acción.
    # Filtro aquellas filas que no posean precio.
    filter_data1 = [{'time_str': x['minute'], 'price': x['close']}
                    for x in dataset if x.get('close') is not None]

    # El tiempo está en formato texto, utilizo los métodos de datetime para
    # pasarlos a objectos tipo datetime
    filter_data2 = [{'time': datetime.strptime(x['time_str'], '%H:%M'),
             'price': x['price']
             }
             for x in filter_data1
            ]

    filter_data3 = [{'x': ((x['time'].hour * 60 + x['time'].minute)),
                     'y': x['price']
                     }
                    for x in filter_data2
                   ]

    yield filter_data3


def plot(dataset):
    x = [data['x'] for data in dataset]
    y = [data['y'] for data in dataset]

    fig = plt.figure()
    ax = fig.add_subplot()
    ax.plot(x, y, c='darkcyan')
    ax.grid()
    # Guarda el plot como imagen
    plt.savefig('finanzas.png')
    plt.close()

    fig = plt.figure()
    ax = fig.add_subplot()
    ax.hist(y, bins=40, color='darkblue')
    ax.grid()
    # Guarda el plot como imagen
    plt.savefig('hist.png')
    plt.close()


def analytics(dataset):
    y = [data['y'] for data in dataset]

    # Cálculo estadístico
    np_array = np.array(y)
    mean = np.mean(np_array)
    std = np.std(np_array)
    print(f'Promedio: {mean:.2f}, desvio: {std:.2f}')
    


def get_graph(**options):
    graph = bonobo.Graph()

    graph.add_chain(extract, transform)
    graph.add_chain(plot, _input=transform)
    graph.add_chain(analytics, _input=transform)
    return graph


def get_services(**options):
    return {}


if __name__ == "__main__":
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
