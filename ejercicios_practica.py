#!/usr/bin/env python
'''
Funciones [Python]
Ejercicios de práctica
---------------------------
Autor: Inove Coding School
Version: 1.1
Descripcion:
Programa creado para poner a prueba los conocimientos
adquiridos durante la clase
'''

__author__ = "Ezequiel Alarcon"
__email__ = "zekalarcon@gmail.com"
__version__ = "1.0"

import bonobo
import time
import requests

# Install bonobo
#   pip3 install -U bonobo 
# Crear archivo "dot"
#   bonobo inspect --graph ejercicios_practica.py > ejercicios_practica.dot
# Graphviz online
#   http://dreampuf.github.io/GraphvizOnline/
# Graphviz (dot) extension
#    Abrir el archivo .dot y presionar CTRL + SHIF + V


def extract():
    # Realice un bucle que recorra una lista del 0 al 10 inclusive
    # En cada iteración de ese bucle realizar un "yield" del valor
    # tomado de la lista
    
    for x in range(11):
        yield x


def transform(x:int):
    # Por cada número que ingrese a transform
    # multiplicarlo por 5

    result = x * 5
    yield result


def load(result):
    # Cada resultado que ingrese a este punto
    # ingresarlo como una nueva linea a un archivo
    # de texto (usando open con 'a' y write)
    # o insertando a una base de datos a elección.
    # El objetivo es que quede almacenado en un archivo
    # o una base de datos la tabla del 5
    
    with open('resultados.txt', 'a') as fi:
        fi.writelines(str(result) + '\n')

    print('Fin!')


def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(extract, transform, load)
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
