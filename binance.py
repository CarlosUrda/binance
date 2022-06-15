#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ejercicio 31 del Capítulo 02.
"""

import utilidades.util as u
import datetime as dt
import sys
import csv




def openCSV(file, mode="r", newline='', dialect=None, isDict=False, fieldnames=None):
    file = open(file, mode, newline)
    # Revisar los modos en open por si hay más que r o w, o con más modificaciones como w+
    if isDict:
        csvParser = csv.DictReader if 'r' in mode else csv.DictWriter
        args = {"dialect":dialect, "fieldnames":fieldnames}
    else: 
        csvParser = csv.reader if 'r' in mode else csv.writer
        args = {"dialect":dialect}

    if dialect is None and 'r' in mode:
        dialect = csv.Sniffer().sniff(file.read(1024))
        file.seek(0)

    # qué sucede si se le pasa dialect=None para saber qué hacer si modo = w: comprobar antes error o dejarlo a csvParser. 
    # o saber qué error da si se intenta file.read sobre un archivo modo w
    return csvParser(file, **args)



def main():
    """ 
    Función principal. 
    """
    # comprobar errores de entrada
    fileNameIn = sys.argv[1]
    fileNameOut = sys.argv[2]

    # Poder meter el formato de la fecha como parámetro en línea de comandos. Por defecto el valor siguiente
    formatoFecha = "%Y-%m-$d %H:%M:%S"

    csvIn = openCSV(fileNameIn, 'r', isDict=True)
    csvOut = openCSV(fileNameOut, 'w', dialect="excel", isDict=True)

    diaPrevio = transaccionesDia = None
    for transaccion in csvHistorial:
        fecha = dt.datetime.strptime(transaccion["UTC_Time"], formatoFecha)
        dia = dt.datetime(fecha.year, fecha.month, fecha.day)
        if (diaPrevio != dia):
            escribirTransaccionesDia(transaccionesDia)
            diaPrevio = dia
            transaccionesDia = dict()
       
       # Saber si la transacción se puede agrupar con otras del mismo día.
       # Si no se agrupa, se crea el nuevo dict o se obtiene el existente modificado
       # Se registra el dict tanto en la lista de transacciones del día como en el dict para saber si ya existe 


    escribirTransaccionesDia(transaccionesDia)
    
    # cerrar el archivo csv abierto







if __name__ in ("__main__", "__console__"):
    main()

