#!/usr/bin/env python
'''
ETL [Python]
Ejemplos de clase
---------------------------
Autor: Inove Coding School
Version: 1.1

Descripcion:
Programa de pruebas con SQL
'''

import json
import sqlite3
import os

import bonobo

import productos

# Install bonobo
#   pip3 install -U bonobo
# NO se puede crear archivo "dot":
# Bonobo no le gusta que tengamos importados nuestras módulos o librerías
# a la hora de generar gráficos, es por eso que en este caso donde se está
# importando "productos" no podemos generar el gráfico tan deseado.


def extract():
    with open('compras.json', 'r') as jsonfile:
        json_data = json.load(jsonfile)
    # Enviar uno a uno los JSON que se obtuvieron
    # del archivo.
    # El "from" hace posible que se envie uno a uno
    yield from json_data


def transform(order):
    client_name = order['nombre']
    productos_id = order['productos']

    productos_str = ''
    productos_total = 0

    # Por cada orden de compra identifico los
    # productos comprados y el total gastado
    for id in productos_id:
        name, price = productos.get_product(id)
        productos_str += name + ' '
        productos_total += price

    yield client_name, productos_str, productos_total


def load(client_name, productos_str, productos_total):
    print('Cliente:', client_name)
    print('Lista de productos:', productos_str)
    print('Total:', productos_total)
    

def get_graph(**options):
    graph = bonobo.Graph()

    graph.add_chain(extract, transform, load)
    return graph


def get_services(**options):
    return {}


if __name__ == "__main__":

    productos.create_schema()
    productos.insert_product('Python', 60)
    productos.insert_product('ETL', 35)
    productos.insert_product('Javascript', 25)
    productos.insert_product('SQL', 20)
    productos.insert_product('Flask', 40)

    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
