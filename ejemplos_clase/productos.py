#!/usr/bin/env python
'''
ETL [Python]
Ejemplos de clase
---------------------------
Autor: Inove Coding School
Version: 1.1

Descripcion:
Programa creado para manipular la base de datos de productos
'''

__author__ = "Inove Coding School"
__email__ = "alumnos@inove.com.ar"
__version__ = "1.1"

import sqlite3


def create_schema():
    conn = sqlite3.connect('productos.db')
    c = conn.cursor()

    # Ejecutar una query
    c.execute("""
                DROP TABLE IF EXISTS producto;
            """)

    # Ejecutar una query
    c.execute(""" 
            CREATE TABLE producto(
                [id] INTEGER PRIMARY KEY AUTOINCREMENT,
                [name] TEXT NOT NULL,
                [price] INTEGER NOT NULL
            );
            """)

    conn.commit()
    conn.close()


def insert_product(name, price):
    conn = sqlite3.connect('productos.db')
    c = conn.cursor()

    values = [name, price]

    c.execute("""
        INSERT INTO producto (name, price)
        VALUES (?,?);""", values)

    conn.commit()
    conn.close()


def get_product(id):
    conn = sqlite3.connect('productos.db')
    c = conn.cursor()

    c.execute("""
        SELECT p.name, p.price FROM producto as p
        WHERE p.id =?;""", [id])

    name, price = c.fetchone()

    conn.commit()
    conn.close()

    return name, price
