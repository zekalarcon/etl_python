#!/usr/bin/env python
'''
ETL [Python]
Ejemplos de clase
---------------------------
Autor: Inove Coding School
Version: 1.1

Descripcion:
Programa de pruebas con archivos CSV
'''

import csv
import os

import bonobo

# Install bonobo
#   pip3 install -U bonobo
# Crear archivo "dot"
#   bonobo inspect --graph 2_etl_csv.py > 2_etl_csv.dot
# Graphviz online
#   http://dreampuf.github.io/GraphvizOnline/
# Graphviz (dot) extension
#    Abrir el archivo .dot y presionar CTRL + SHIF + V


csv_file = 'propiedades.csv'
reporte = 'reporte.csv'

def extract():
    with open(csv_file, 'r') as fi:
        reader = csv.DictReader(fi)
        for row in reader:
            # Leer linea a linea el archivo CSV e enviar al siguiente proceso
            yield row


def transform(row):
    # Descartar los alquileres que no esten en pesos
    if row.get('moneda') == 'ARS':
        yield row


def load(row):
    with open(reporte, 'a', newline='') as fo:
        # Escribir resultado de alquileres transformados 
        # en un archivo CSV de salida (sin header)
        writer = csv.writer(fo)
        writer.writerow(row.values())


def get_graph(**options):
    # Elimino el archivo de reporte para empezar de cero
    if os.path.isfile(reporte) is True:
        os.remove(reporte)

    graph = bonobo.Graph()
    graph.add_chain(
        extract,
        # Limito la cantidad de informacion que fluye al siguiente eslavon
        bonobo.Limit(20),
        transform,
        load
        )
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
