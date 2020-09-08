#!/usr/bin/env python
'''
ETL [Python]
Ejemplos de clase
---------------------------
Autor: Inove Coding School
Version: 1.1

Descripcion:
Programa "hello world" de ETL con Bonobo
'''

__author__ = "Inove Coding School"
__email__ = "alumnos@inove.com.ar"
__version__ = "1.1"

import bonobo
import time
import requests

# Crear archivo "dot"
#   bonobo inspect --graph 1_etl_hello_world.py > 1_etl_hello_world.dot
# Graphviz online
#   http://dreampuf.github.io/GraphvizOnline/
# Graphviz (dot) extension
#    Abrir el archivo .dot y presionar CTRL + SHIF + V


def extract():
    print('Extrae hello')
    yield 'hello'
    time.sleep(1)
    print('Extrae world')
    yield 'world'


def transform(x: str):
    print('Transforma:', x)
    yield x.capitalize()


def load(text):
    print('Carga:', text)


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
